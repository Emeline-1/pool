package pool

import (
    "log"
	"sync"
    "fmt"
    "io/ioutil"
    "bufio"
    "os"
    "context"
    "os/signal"
    "syscall"
    //"strconv"
)

/* --- Abstract type representing the workload --- */
type Work_load interface {}

/**
 * Simulates a pool of worker:
 *
 * - n: the number of workers
 * - f: function that will do the work (takes a string as argument)
 * - workload:
 *         The workload can be given as a slice of strings or a map of strings. Function f will be called on each string of the slice/map.
 *         The workload can be given as a file. Function f will be called on each line of the file.
 */
func Launch_pool (n int, workload Work_load, f func (s string)) {
	wait_group := new(sync.WaitGroup)
    wait_group.Add(n)
    job_channel := make (chan string) // Send jobs to the goroutines
    update_channel := make (chan struct{}) // Receive notification update from the goroutines

    //defer close(job_channel) // This will cause goroutines to exit the loop on the job channel and return

    /* --- Launch the n workers --- */
    for i := 0; i < n; i++ {
        go worker(job_channel, update_channel, wait_group, f, i)
    }

    /* --- Launch the update mechanism --- */
    my_channel := make (chan struct{})
    defer close (my_channel)
    go func () {
        fmt.Fprintf(os.Stderr, "\r\033[sProcessing: 0") // save the cursor position
        counter := 0
        for range update_channel {
            counter++
            fmt.Fprintf(os.Stderr, "\r\033[u\033[KProcessing: %v", counter) // restore the cursor position and clear the line
        }
        fmt.Fprintf(os.Stderr, "\n")
        my_channel <- struct{}{} // Notifies the update printing mechanism is done.
    } ()
    

    /* --- Graceful shutdown --- */
    termChan := make(chan os.Signal)
    signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM) 
    /* NOTE: The shell will signal the entire process group when you press ctrl+c. Children will thus be terminated as well.
       To prevent the shell from signaling the children, you need to start the command in its own process group 
       with the Setpgid and Pgid fields in syscall.SysProcAttr before starting the processes.
       https://stackoverflow.com/questions/33165530/prevent-ctrlc-from-interrupting-exec-command-in-golang

       https://bigkevmcd.github.io/go/pgrp/context/2019/02/19/terminating-processes-in-go.html (interesting sample code)
     */
    ctx, cancelFunc := context.WithCancel(context.Background())
    go func () {
        <-termChan //Blocking
        cancelFunc() // Send shutdown signal through context (ctx.Done () will fire)
    } ()

    /* --- Send jobs to the worker --- */
    done_channel := make (chan struct{})
    intermediate_channel := make (chan string)

    go start_producer (workload, intermediate_channel, done_channel)
    start_consumer (job_channel, intermediate_channel, ctx, done_channel) // Don't exit until all jobs have been assigned to workers
    
    wait_group.Wait() // Wait for all goroutines to finish
    close (update_channel) // Will cause the loop on update_channel to exit.
    <-my_channel // Do not return until the update mechanism is not done.
}

func worker(job_channel <-chan string, update_channel chan<- struct{}, wait_group *sync.WaitGroup, f func (s string), nb int) {
    // Notifies work pool when a worker has returned
    defer wait_group.Done()

    /* --- Consumes jobs as long as the pool doesn't close the job channel --- */
    for job := range job_channel {
        f (job)
        update_channel <- struct{}{} // Notifies one file has been parsed
    }
}

/** 
 * Sends jobs to the workers. Can handle SIGTERM
 * - job_channel: channel on which to send the jobs to the workers
 * - intermediate_channel: channel from which we receive the jobs to transmit to the workers
 * - ctx: channel notifying us in case of SIGTERM
 * - done_channel: allows to exit the function
 */
func start_consumer (job_channel chan<- string, intermediate_channel <-chan string, ctx context.Context, done_channel <-chan struct{}) {
    defer close (job_channel)
    for {
        select {
            case job := <-intermediate_channel: /* --- Transfer job from intermediate channel to job channel --- */
                job_channel <- job
            case <-ctx.Done(): /* --- Exit when SIGTERM is received --- */
                log.Print ("\n*******\nShutdown signal received\n*********\n")
                return
            case <-done_channel: /* --- Exit when producer has transmitted all jobs to the intermediate channel --- */
                return
        }
    }
}

/**
 * Sends jobs through an intermediate channel (not directly through the job_channel)
 * - workload: the workload representing the jobs
 * - intermediate_channel: channel on which to send the jobs
 * - done_channel: notifies that producer is done sending jobs on intermediate_channel
 */
func start_producer (workload Work_load, intermediate_channel chan<- string, done_channel chan<- struct{}) {

    switch jobs := workload.(type) {
        /* ---------------------------------- */
        case map[string]struct{}:
            for job, _ := range jobs {
                intermediate_channel <- job //Blocking
            }
        /* ---------------------------------- */
        case []string:
            for _, job := range jobs {
                intermediate_channel <- job //Blocking
            }
        /* ---------------------------------- */
        case string:
            file, err := os.Open(jobs) // Read only
            defer file.Close()
            if err != nil {
                log.Print ("[start_producer]: " + err.Error())
                return
            }
            scanner := bufio.NewScanner(file)
            for scanner.Scan() {
                intermediate_channel <- scanner.Text ()
            }
        /* ---------------------------------- */
        default:
            log.Printf("[start_producer]: Unknown workload %T!\n", workload)
    }
    done_channel<-struct{}{}
    //NB: don't close intermediate_channel or there will be a race condition, as a closed channel is always ready for communication (and sends the nul value)
}

/**
 * Given a directory, list all the contents of that directory and return it in a slice of strings
 */
func Get_directory_files (directory string) *[]string {
    files, err := ioutil.ReadDir(directory)
    if err != nil {
        log.Print ("[get_directory_files]: " + err.Error())
        return nil
    }

    s := make ([]string, len (files))
    for i, file := range files {
        s[i] = directory + "/" + file.Name()
    }
    return &s
}
