package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"mapreduce/constants"
	pb "mapreduce/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	SEND_HEARTBEAT_TIMEOUT         = 5 * time.Second
	REQUEST_TASK_SLEEP             = 5 * time.Second
	REQUEST_TASK_TIMEOUT           = 10 * time.Second
	NO_TASK_SLEEP                  = 2 * time.Second
	REPORT_TASK_COMPLETION_TIMEOUT = 10 * time.Second
)

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MapFunction func(filename string, contents string) []KeyValue

type ReduceFunction func(key string, values []string) string

type Worker struct {
	pb.UnimplementedWorkerServiceServer

	ID            string
	masterAddress string
	masterClient  pb.MasterServiceClient
	port          int

	mu          sync.RWMutex
	currentTask *pb.Task
	isRunning   bool

	mapFunc    MapFunction
	reduceFunc ReduceFunction
	workDir    string

	shutdownChan chan struct{}
}

func NewWorker(id string, masterAddress string, port int) *Worker {
	return &Worker{
		ID:            id,
		masterAddress: masterAddress,
		port:          port,
		workDir:       fmt.Sprintf("/tmt/worker-%s", id),
		shutdownChan:  make(chan struct{}),
	}
}

func (w *Worker) connectToMaster() error {
	options := grpc.WithTransportCredentials(insecure.NewCredentials())
	client, err := grpc.NewClient(w.masterAddress, options)
	if err != nil {
		return fmt.Errorf("Worker %s failed to connect to master; %v", w.ID, err)
	}
	w.masterClient = pb.NewMasterServiceClient(client)
	return nil
}

func (w *Worker) registerWithMaster() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.RegisterWorkerRequest{
		WorkerId:      w.ID,
		WorkerAddress: fmt.Sprintf("worker-%s-%d", w.ID, w.port),
	}
	resp, err := w.masterClient.RegisterWorker(ctx, req)
	if err != nil {
		return fmt.Errorf("Worker %s failed to register with master; %v", w.ID, err)
	}

	if !resp.Success {
		return fmt.Errorf("Worker %s was rejeted registration with master; %v", w.ID, err)
	}

	log.Printf("Worker %s succesfully connected to master", w.ID)
	return nil
}

// TODO: send in the function name
func (w *Worker) Start(mapFunc MapFunction, reduceFunc ReduceFunction) error {
	if err := os.MkdirAll(w.workDir, 0o755); err != nil {
		return fmt.Errorf("Worker %s failed to create work direcory", w.ID)
	}

	if err := w.connectToMaster(); err != nil {
		return err
	}
	if err := w.registerWithMaster(); err != nil {
		return err
	}
	w.mapFunc = mapFunc
	w.reduceFunc = reduceFunc

	go w.startServer()
	go w.startHeartbeat()
	w.runTaskLoop()

	return nil
}

func (w *Worker) startServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", w.port))
	if err != nil {
		log.Printf("Worker %s failed t o start server; %v", w.ID, err)
	}

	s := grpc.NewServer()
	pb.RegisterWorkerServiceServer(s, w)
	log.Printf("Worker %s server starting on port %d", w.ID, w.port)
	if err := s.Serve(lis); err != nil {
		log.Printf("Worker server failed: %v", err)
	}
}

func (w *Worker) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Message:   "pong",
		Timestamp: time.Now().Unix(),
	}, nil
}

func (w *Worker) startHeartbeat() {
	ticker := time.NewTicker(constants.HEARTBEAT_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.sendHeartbeat()
		case <-w.shutdownChan:
			return
		}
	}
}

func (w *Worker) sendHeartbeat() {
	w.mu.RLock()
	currentTask := w.currentTask
	w.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), SEND_HEARTBEAT_TIMEOUT)
	defer cancel()

	req := &pb.HeartbeatRequest{
		WorkerId: w.ID,
	}

	if currentTask != nil {
		req.CurrentTaskStatus = currentTask.Status
		req.CurrentTaskId = currentTask.TaskId
	}

	_, err := w.masterClient.Heartbeat(ctx, req)
	if err != nil {
		log.Printf("Worker %s failed to send heartbeat; %v", w.ID, err)
	}
}

func (w *Worker) runTaskLoop() {
	for w.isRunning {
		task, err := w.requestTask()
		if err != nil {
			log.Printf("Worker %s failed to get a task; %v", w.ID, err)
			time.Sleep(REQUEST_TASK_SLEEP)
			continue
		}

		if task == nil {
			time.Sleep(NO_TASK_SLEEP)
			continue
		}
		w.executeTask(task)
	}
}

func (w *Worker) requestTask() (*pb.Task, error) {
	ctx, cancel := context.WithTimeout(context.Background(), REQUEST_TASK_TIMEOUT)
	defer cancel()

	req := &pb.GetTaskRequest{
		WorkerId: w.ID,
	}

	res, err := w.masterClient.GetTask(ctx, req)
	if err != nil {
		return nil, err
	}

	if res.JobComplete {
		log.Printf("Worker %s finished all jobs; shutting down", w.ID)
		w.isRunning = false
		return nil, nil
	}

	if !res.HasTask {
		return nil, nil
	}

	return res.Task, nil
}

func (w *Worker) executeTask(task *pb.Task) {
	w.mu.Lock()
	w.currentTask = task
	w.mu.Unlock()

	var outputFiles []string
	var success bool
	var errorMsg string
	switch task.Type {
	case pb.TaskType_MAP:
		outputFiles, success, errorMsg = w.executeMapTask(task)
	case pb.TaskType_REDUCE:
		outputFiles, success, errorMsg = w.executeReduceTask(task)
	default:
		success = false
		errorMsg = fmt.Sprintf("Worker %s failed - unknown task type %v", w.ID, task.Type)
	}

	w.reportTaskCompletion(task.TaskId, success, errorMsg, outputFiles)

	w.mu.Lock()
	w.currentTask = nil
	w.mu.Unlock()
}

func (w *Worker) executeMapTask(task *pb.Task) ([]string, bool, string) {
	var outputFiles []string

	for _, inputFile := range task.InputFiles {
		content, err := os.ReadFile(inputFile)
		if err != nil {
			return nil, false, fmt.Sprintf("Worker %s failed to read file %s; %v", w.ID, inputFile, err)
		}

		kvPairs := w.mapFunc(inputFile, string(content))

		partitions := make(map[int][]KeyValue)
		for _, kv := range kvPairs {
			partition := w.hash(kv.Key) % int(task.NumReduceTask)
			partitions[partition] = append(partitions[partition], kv)
		}

		for partition, pairs := range partitions {
			filename := fmt.Sprintf("%s/mr-%s-%d", w.workDir, task.TaskId, partition)
			if err := w.writeIntermediateFile(filename, pairs); err != nil {
				return nil, false, fmt.Sprintf("Worker %s failed to write intermediate file; %v", w.ID, err)
			}
			outputFiles = append(outputFiles, filename)
		}
	}
	return outputFiles, true, ""
}

func (w *Worker) executeReduceTask(task *pb.Task) ([]string, bool, string) {
	intermediateFiles, err := w.findIntermediateFiles(task.ReduceTaskNum)
	if err != nil {
		return nil, false, fmt.Sprintf("Worker %s failed to find intermediate files; %v", w.ID, err)
	}

	keyValues := make(map[string][]string)
	for _, file := range intermediateFiles {
		kvPairs, err := w.readIntermediateFile(file)
		if err != nil {
			return nil, false, fmt.Sprintf("Worker %s failed to read intermediate file; %v", w.ID, err)
		}

		for _, kv := range kvPairs {
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}

	var keys []string
	for key := range keyValues {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	outputFile := fmt.Sprintf("%s/mr-out-%d", task.OutputDir, task.ReduceTaskNum)
	file, err := os.Create(outputFile)
	if err != nil {
		return nil, false, fmt.Sprintf("Worker %s failed to create output file; %v", w.ID, err)
	}
	defer file.Close()

	for _, key := range keys {
		values := keyValues[key]
		result := w.reduceFunc(key, values)
		fmt.Fprintf(file, "%s %s\n", key, result)
	}

	return []string{outputFile}, true, ""
}

func (w *Worker) writeIntermediateFile(filename string, pairs []KeyValue) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, pair := range pairs {
		if err := encoder.Encode(pair); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) readIntermediateFile(filename string) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var pairs []KeyValue
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var pair KeyValue
		if err := decoder.Decode(&pair); err != nil {
			return nil, err
		}
		pairs = append(pairs, pair)
	}
	return pairs, nil
}

func (w *Worker) findIntermediateFiles(partition int32) ([]string, error) {
	var files []string

	// TODO: zadziała dla * wielocyfrowej?
	pattern := fmt.Sprintf("%s/mr-*-%d", w.workDir, partition)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	files = append(files, matches...)

	sharedPattern := fmt.Sprintf("/tmp/mapreduce-intermediate/mr-*-%d", partition)
	sharedMatches, err := filepath.Glob(sharedPattern)
	if err == nil {
		files = append(files, sharedMatches...)
	}
	return files, nil
}

func (w *Worker) hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32())
}

func (w *Worker) reportTaskCompletion(taskID string, success bool, errorMsg string, outputFiles []string) {
	ctx, cancel := context.WithTimeout(context.Background(), REPORT_TASK_COMPLETION_TIMEOUT)
	defer cancel()

	req := &pb.TaskCompleteRequest{
		WorkerId:    w.ID,
		TaskId:      taskID,
		Success:     success,
		ErrorMsg:    errorMsg,
		OutputFiles: outputFiles,
	}

	_, err := w.masterClient.CompleteTask(ctx, req)
	if err != nil {
		log.Printf("Worker %s failed to report task completion; %v", w.ID, err)
	} else {
		log.Printf("Worker %s succesfully reported task completion; %v", w.ID, err)
	}
}

func (w *Worker) Shutdown() {
	w.isRunning = false
	close(w.shutdownChan)
}

func main() {
	if len(os.Args) != 4 {
		log.Fatal("Usage: worker <worker-id> <master-address> <port>")
	}
	
	workerID := flag.String("id", "", "worker id")
	masterAddress := flag.String("maddr", "", "master address")
	port := flag.Int("port", 0, "worker port")

	if *workerID == "" {
		log.Fatalf("Pass -id arg")
	}
	if *masterAddress == "" {
		log.Fatalf("pass -madd arg")
	}
	if *port == 0 {
		log.Fatalf("pass -port arg")
	}
	
	worker := NewWorker(*workerID, *masterAddress, *port)
	if err := worker.Start(wordCountMap, wordCountReduce); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
}
