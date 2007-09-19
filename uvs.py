# uvs.py
# display the number of UVs Posdo has connected
def job_init(args) : 
    if posdo.uvs_nof() == 0:
        print 'Posdo has 0 uvs'
    return 0

def job_get_globals() : return ''

def job_get_arg(task_num) : return None
    
def job_add_result(task_num, result) : pass

def job_notify_failure(task_num) : pass

def job_finish() : 
 print 'Posdo has %d uvs' % (posdo.uvs_nof())
       
def job_worker(arg) : pass

