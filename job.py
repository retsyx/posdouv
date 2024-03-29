# job.py
# Simple job to demonstrate posdo operation
# Worker sleeps for a second and then prints it performed a task
i = 0

def job_init(args):
    """
        Initialize the job.
        Returns 0 on success.
    """
    global i
    i = 0
    posdo.info('running with %d uvs' % (posdo.uvs_nof()))
    return 0

#def job_get_options(): return ...

def job_get_globals():
    """
        Get job globals to be used on all UVs.
        Return empty string to signify no globals.
    """
    return ''

def job_get_status():
    """ Obtain job status (optional).
        Returns:
        0, 0 - job complete
        1, N - job not complete, require N more results before job_get_arg() can be called
    """
    global i
    if i == 10: return 0, 0
    return 1, 0
    
def job_get_arg(task_num):
    """
        Get a job argument/task to be executed on a UV.
        Returns None to signify no more arguments/tasks are left
        and that this job is done (optional if job_get_status() is defined).
    """
    global i 
    if i == 10: return None # return None to signal the end of the job
    arg = str(i)
    i = i + 1
    return arg

def job_add_result(task_num, result):
    """
        Process a result of an executed argument/task
    """    
    posdo.info(result)

def job_notify_failure(task_num): 
    """
        Process an argument/task execution failure (optional)
    """
    pass

def job_finish():
    """
        Called when all job processing is complete.
    """    
    posdo.info('job completed')

# Posdo will remote job_worker() and below to UV
       
def job_worker(arg):
        return 'internally ' + job_internal_call(arg)

def job_internal_call(arg):
        import time
        time.sleep(1)
        result = 'performed task %s' % (arg)
        return result

