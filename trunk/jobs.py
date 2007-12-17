# jobs.py
# display running jobs
def job_init(args): return 0

def job_get_globals(): return ''

def job_get_arg(task_num): return None
    
def job_add_result(task_num, result): pass

def job_notify_failure(task_num): pass

def job_finish():
 posdo.info('Posdo has %d jobs' % (posdo.jobs_nof()-1))
       
def job_worker(arg): pass
