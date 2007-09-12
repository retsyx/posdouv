# uvs.py
# display the number of UVs Posdo has connected
def job_init(args) : return 0

def job_get_globals() : return ''

def job_get_arg(task_num) : return None
    
def job_add_result(task_num, result) : pass

def job_notify_failure(task_num) : pass

def job_finish() : 
 global uvs # XXX reaching!
 print len(uvs)
       
def job_worker(arg) : pass

