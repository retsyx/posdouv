# kill.py
# Kill all UVs and shutdown posdo
import sys

nof_uvs = 0
nof_kills = -1

def job_init(args):
    global nof_uvs
    nof_uvs = posdo.uvs_nof()
    posdo.info('network of %d' % (nof_uvs))
    if nof_uvs == 0: job_notify_failure(0) 
    return 0

def job_get_options():
    return (0, 0, 0) # no power scaling, no task redo, unlimited max outstanding
    
def job_get_globals():
    return ''

def job_get_arg(task_num):
    global nof_kills 
    nof_kills = task_num
    return task_num

def job_add_result(task_num, result): pass
    
def job_notify_failure(task_num): 
    global nof_uvs, nof_kills
    if posdo.uvs_nof() == 0: 
        posdo.info('killed network of %d with %d kills' % (nof_uvs, nof_kills+1))
        posdo.terminate()
    
def job_finish():
    posdo.terminate()
       
def job_worker(arg):
    import sys
    sys.exit(0)
