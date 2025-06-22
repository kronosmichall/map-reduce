package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"mapreduce/constants"
	pb "mapreduce/proto"

	"google.golang.org/grpc"
)

type Job struct {
	ID             string
	InputFiles     []string
	OutputDir      string
	NumReduceTasks int32
	MapFunction    string
	ReduceFunction string
	Status         string
	MapTasks       []*pb.Task
	ReduceTasks    []*pb.Task
	CreatedAt      time.Time
	CompletedAt    *time.Time
}

type Worker struct {
	ID          string
	Address     string
	LastSeen    time.Time
	CurrentTask *pb.Task
	IsAlive     bool
}

type Master struct {
	pb.UnimplementedMasterServiceServer

	mu             sync.Mutex
	jobs           map[string]*Job
	workers        map[string]*Worker
	pendingTasks   []*pb.Task
	runningTasks   map[string]*pb.Task
	completedTasks map[string]*pb.Task
	failedTasks    map[string]*pb.Task

	taskTimeout       time.Duration
	heartbeatInterval time.Duration

	taskChan     chan *pb.Task
	shutdownChan chan struct{}
}

func newMaster() *Master {
	return &Master{
		jobs:              make(map[string]*Job),
		workers:           make(map[string]*Worker),
		runningTasks:      make(map[string]*pb.Task),
		completedTasks:    make(map[string]*pb.Task),
		failedTasks:       make(map[string]*pb.Task),
		taskTimeout:       5 * time.Minute,
		heartbeatInterval: constants.HEARTBEAT_INTERVAL,
		taskChan:          make(chan *pb.Task, 1000),
		shutdownChan:      make(chan struct{}),
	}
}

func (m *Master) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("Registering worker %s at %s", req.WorkerId, req.WorkerAddress)

	worker := &Worker{
		ID:       req.WorkerId,
		Address:  req.WorkerAddress,
		LastSeen: time.Now(),
		IsAlive:  true,
	}

	m.workers[req.WorkerId] = worker

	return &pb.RegisterWorkerResponse{
		Success: true,
		Msg:     "Worker registered succesfully",
	}, nil
}

func (m *Master) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.GetTaskResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if worker, exists := m.workers[req.WorkerId]; exists {
		worker.LastSeen = time.Now()
	}

	if len(m.pendingTasks) == 0 {
		allComplete := true
		for _, job := range m.jobs {
			if job.Status != "completed" && job.Status != "failed" {
				allComplete = false
				break
			}
		}
		return &pb.GetTaskResponse{
			HasTask:     false,
			JobComplete: allComplete,
		}, nil
	}

	task := m.pendingTasks[0]
	m.pendingTasks = m.pendingTasks[1:]

	task.Status = pb.TaskStatus_IN_PROGRESS
	task.AssignedAt = time.Now().Unix()
	m.runningTasks[task.TaskId] = task

	if worker, exists := m.workers[req.WorkerId]; exists {
		worker.CurrentTask = task
	} else {
		return nil, fmt.Errorf("Failed to find worker %s", req.WorkerId)
	}
	return &pb.GetTaskResponse{
		HasTask:     true,
		Task:        task,
		JobComplete: false,
	}, nil
}

func (m *Master) CompleteTask(ctx context.Context, req *pb.TaskCompleteRequest) (*pb.TaskCompleteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("Task %s completed by worker %s, success: %v", req.TaskId, req.WorkerId, req.Success)

	task, exists := m.runningTasks[req.TaskId]
	if !exists {
		return &pb.TaskCompleteResponse{
			Success: false,
		}, nil
	}

	delete(m.runningTasks, req.TaskId)

	if worker, exists := m.workers[req.WorkerId]; exists {
		worker.CurrentTask = nil
	}

	if req.Success {
		task.Status = pb.TaskStatus_COMPLETED
		m.completedTasks[req.TaskId] = task

		m.checkJobProgress(task)
	} else {
		task.Status = pb.TaskStatus_FAILED
		m.failedTasks[req.TaskId] = task

		m.rescheduleTask(task)
	}
	return &pb.TaskCompleteResponse{
		Success: true,
	}, nil
}

func (m *Master) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if worker, exists := m.workers[req.WorkerId]; exists {
		worker.LastSeen = time.Now()
		worker.IsAlive = true
	}
	return &pb.HeartbeatResponse{
		KeepAlive: true,
	}, nil
}

func (m *Master) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (*pb.SubmitJobResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("Received job submission: %s", req.JobId)

	job := &Job{
		ID:             req.JobId,
		InputFiles:     req.InputFiles,
		OutputDir:      req.OutDir,
		NumReduceTasks: req.NumReduceTasks,
		MapFunction:    req.MapFunction,
		ReduceFunction: req.ReduceFunction,
		Status:         "pending",
		CreatedAt:      time.Now(),
	}

	job.MapTasks = m.CreateMapTasks(job)
	job.ReduceTasks = m.CreateReduceTasks(job)
	m.jobs[req.JobId] = job

	for _, task := range job.MapTasks {
		m.pendingTasks = append(m.pendingTasks, task)
	}
	job.Status = "running"
	return &pb.SubmitJobResponse{
		Accepted: true,
		Msg:      "Job accepted and queued",
		JobId:    req.JobId,
	}, nil
}

func (m *Master) GetJobStatus(ctx context.Context, req *pb.JobStatusRequest) (*pb.JobStatusResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	job, exists := m.jobs[req.JobId]
	if !exists {
		return nil, fmt.Errorf("Job %s not found", req.JobId)
	}

	totalTasks := len(job.MapTasks) + len(job.ReduceTasks)
	completedTasks := 0
	failedTasks := 0

	for _, task := range job.MapTasks {
		if task.Status == pb.TaskStatus_COMPLETED {
			completedTasks++
		} else if task.Status == pb.TaskStatus_FAILED {
			failedTasks++
		}
	}

	return &pb.JobStatusResponse{
		JobId:          req.JobId,
		Status:         job.Status,
		TotalTasks:     int32(totalTasks),
		CompletedTasks: int32(completedTasks),
		FailedTasks:    int32(failedTasks),
	}, nil
}

func (m *Master) createMapTasks(job *Job) []*pb.Task {
	tasks := []*pb.Task{}

	for i, inputFile := range job.InputFiles {
		task := &pb.Task{
			TaskId:        fmt.Sprintf("%s-map-%d", job.ID, i),
			Type:          pb.TaskType_MAP,
			Status:        pb.TaskStatus_PENDING,
			InputFiles:    []string{inputFile},
			OutputDir:     job.OutputDir,
			NumReduceTask: job.NumReduceTasks,
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func (m *Master) CreateReduceTasks(job *Job) []*pb.Task {
	tasks := []*pb.Task{}

	for i := int32(0); i < job.NumReduceTasks; i++ {
		task := &pb.Task{
			TaskId:        fmt.Sprintf("%s-reduce-%d", job.ID, i),
			Type:          pb.TaskType_REDUCE,
			Status:        pb.TaskStatus_PENDING,
			OutputDir:     job.OutputDir,
			ReduceTaskNum: i,
			NumReduceTask: job.NumReduceTasks,
		}
		tasks = append(tasks, task)
	}
	return tasks
}

func (m *Master) checkJobProgress(completedTask *pb.Task) {
	getJob := func() *Job {
		for _, j := range m.jobs {
			if completedTask.Type == pb.TaskType_MAP {
				for _, task := range j.MapTasks {
					if task.TaskId == completedTask.TaskId {
						return j
					}
				}
			} else {
				for _, task := range j.ReduceTasks {
					if task.TaskId == completedTask.TaskId {
						return j
					}
				}
			}
		}
		return nil
	}
	job := getJob()
	if job == nil {
		return
	}

	if completedTask.Type == pb.TaskType_MAP {
		allMapComplete := true
		for _, task := range job.MapTasks {
			if task.Status != pb.TaskStatus_COMPLETED {
				allMapComplete = false
				break
			}
		}

		if allMapComplete {
			log.Printf("All map tasks complete of job %s ; scheduling reduce tasks", job.ID)
			for _, task := range job.ReduceTasks {
				m.pendingTasks = append(m.pendingTasks, task)
			}
		}
	} else {
		allReduceComplete := true
		for _, task := range job.ReduceTasks {
			if task.Status != pb.TaskStatus_COMPLETED {
				allReduceComplete = false
				break
			}
		}

		if allReduceComplete {
			log.Printf("Job %s completed succesfully", job.ID)
			job.Status = "completed"
			now := time.Now()
			job.CompletedAt = &now
		}
	}
}

// TODO:  exponential backoff
func (m *Master) rescheduleTask(task *pb.Task) {
	task.Status = pb.TaskStatus_PENDING
	m.pendingTasks = append(m.pendingTasks, task)
	log.Printf("Rescheduling failed task: %s", task.TaskId)
}

func (m *Master) startHealthChecker() {
	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkWorkerHealth()
		case <-m.shutdownChan:
			return
		}
	}
}

func (m *Master) checkWorkerHealth() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for _, worker := range m.workers {
		if now.Sub(worker.LastSeen) > 2*m.heartbeatInterval {
			log.Printf("Worker %s appears to be dead", worker.ID)
			worker.IsAlive = false

			if worker.CurrentTask != nil {
				log.Printf("Rescheduling task %s of dead worker %d", worker.CurrentTask.TaskId, worker.ID)
				delete(m.runningTasks, worker.CurrentTask.TaskId)
				m.rescheduleTask(worker.CurrentTask)
				worker.CurrentTask = nil
			}
		}
	}
}

func (m *Master) Start(port int) error {
	go m.startHealthChecker()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterMasterServiceServer(server, m)

	log.Printf("Master server starting on port %d", port)
	return server.Serve(lis)
}

func main() {
	port := flag.Int("port", 50051, "port of the master server")
	master := newMaster()
	if err := master.Start(*port); err != nil {
		log.Fatalf("Failed to start master: %v", err)
	}
}
