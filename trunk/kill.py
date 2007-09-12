# kill.py
# Kill all UVs and shutdown posdo
import sys

nof_kills = -1

def job_init(args) :
    if get_nof_uvs() == 0 : job_notify_failure(0) 
    return 0

def job_get_options() :
    return (0, 0, 0) # no power scaling, no task redo, unlimited max outstanding
    
def job_get_globals() :
    return ''

def job_get_arg(task_num) :
    global nof_kills 
    nof_kills = task_num
    return task_num

def job_add_result(task_num, result) : pass
    
def job_notify_failure(task_num) : 
    if get_nof_uvs() <= 1 : 
        print 'killed network of', nof_kills + 1
        sys.exit(0)

def get_nof_uvs() :
    global uvs # XXX reaching into Posdo's guts
    return len(uvs)
    
def job_finish() :
    sys.exit(0) 
       
def job_worker(arg) :
    import sys
    sys.exit(0)
