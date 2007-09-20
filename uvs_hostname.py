# uvs_hostname.py
# Get hostnames of all uvs in the network

uvs_nof = 0
failures_nof = 0
results_nof = 0

def job_init(args):
    global uvs_nof
    uvs_nof = posdo.uvs_nof()
    return 0

def job_get_options():
    return (0, 0, 0) # no power scaling, no task redo, unlimited max outstanding

def job_get_globals():
    return ''

def job_get_arg(task_num):
    global uvs_nof
    if task_num < uvs_nof: return task_num
    return None
    
def job_add_result(task_num, result):
    global results_nof
    results_nof += 1
    posdo.info(result)

def job_notify_failure(task_num):
    global failures_nof
    failures_nof += 1

def job_finish():
    global uvs_nof, failures_nof, results_nof
    posdo.info('%d uvs, %d replied, %d failed' % (uvs_nof, results_nof, failures_nof))

# Posdo will remote job_worker() and below to UV
       
def job_worker(arg):
    import socket
    return socket.gethostname()


