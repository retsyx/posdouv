import inspect
import sys

job_filename = sys.argv[1]
job_args = sys.argv[2:]

job_file = open(job_filename, 'r')
x = compile(job_file.read(), job_filename, 'exec')
job_file.close()
exec(x)

# find job_worker() and suck all the source starting with it
job_worker_lines = inspect.findsource(job_worker)
job_worker_str = ''.join(job_worker_lines[0][job_worker_lines[1]:])

# initialize the job
if job_init(job_args) :
    print 'job_init() failed'
    sys.exit(1)

globals = job_get_globals()

nof_tasks = 0
while 1 :
    # get an argument for a task
    arg = job_get_arg(nof_tasks)
    if arg == '' : break
    
    # execute the task
    result = job_worker(arg)
    
    # add the result
    job_add_result(nof_tasks, result)

    nof_tasks = nof_tasks + 1
    
# job is done    
job_finish()