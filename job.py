# job.py
# Simple job to demonstrate posdo operation
# Worker sleeps for a second and then prints it performed a task
i = 0

def job_init(args) :
 global i
 i = 0
 return 0

#def job_get_options() : return ...

def job_get_globals() :
    return ''

def job_get_arg(task_num) :
    global i 
    if i == 10 : return None # return None to signal the end of the job
    arg = str(i)
    i = i + 1
    return arg

def job_add_result(task_num, result) :
    print result

def job_notify_failure(task_num) : pass

def job_finish() :
 pass

# Posdo will remote job_worker() and below to UV
       
def job_worker(arg) :
        return 'internally ' + job_internal_call(arg)

def job_internal_call(arg) :
        import time
        time.sleep(1)
        result = 'performed task %s' % (arg)
        return result

