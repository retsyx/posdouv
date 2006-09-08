import inspect
import select
import socket
import sys
import time

# Format
#
# Clinet->Server
# <connect>
# Server->Client
# Length\n
# State: (task base, [task_args])
# Code
# Client->Server
# Length\n
# Result: (task base, [task_results])
# <repeat>/<disconnect>

dbg_lvl = 3

def dbg_out(lvl, s) :
    global dbg_lvl
    if lvl <= dbg_lvl : print ''.join([str(x) for x in s])

def dmp(s) : dbg_out(5, s)
def dbg(s) : dbg_out(4, s)    
def info(s) : dbg_out(3, s)
def wrn(s) : dbg_out(2, s)
def err(s) : dbg_out(1, s)

class struct : 
    def __str__(self) :
        return str(self.__dict__)
    def __repr__(self) :
        return str(self.__dict__)

def so_read_line(so) :
    s = ''
    t = so.recv(1)
    while (t and t != '\n') :
        s = s + t
        t = so.recv(1)

    return s
    
def so_read_block(so) :
    # read length
    s = so_read_line(so)
    try :
        lblk = long(s)
    except :
        lblk = 0
    
    # if nothing to read, quit
    if lblk == 0 : return ''

    # read payload
    blk = []
    blk_read = 0
    while blk_read < lblk :
        part = so.recv(lblk)
        blk.append(part)
        blk_read = blk_read + len(part)
        
    blk = ''.join(blk)
    dmp(('=>', s, '\n', blk))
    return blk

def so_read_task(so) :
    s = so_read_block(so)
    task_info, task = s.split('\n', 1)
    return eval(task_info), task

def so_write_block(so, s) :
    t = str(len(s)) + '\n' + s
    dmp(('<=', t))
    so.send(t)

def so_write_task(so, s) :
    task_info, s = s
    s = str(task_info) + '\n' + s
    so_write_block(so, s)
    


port = long(sys.argv[1])
job_filename = sys.argv[2]
job_args = sys.argv[3:]

job_file = open(job_filename, 'r')
x = compile(job_file.read(), job_filename, 'exec')
job_file.close()
exec(x)
#job_worker_str = inspect.getsource(job_worker) 
# find job_worker() and suck all the source starting with it
job_worker_lines = inspect.findsource(job_worker)
job_worker_str = ''.join(job_worker_lines[0][job_worker_lines[1]:])

# initialize the job
job_init(job_args)

min_time_per_task_sec = 10
max_time_per_task_sec = 60

uvs = {}
outstanding_tasks = {} # elements of the form (task_base, task_len)
redo_tasks = [] # elements of the form (task_base, task_len)

nof_tasks = 0
iwtd = []
owtd = []
ewtd = []
host = ''
sol = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sol.bind((host, port))
sol.listen(3)
iwtd.append(sol)

done = 0
while not done :
    ri, ro, re = select.select(iwtd, owtd, ewtd, 1)
    
    now = time.time()
    
    for so in ri :
        try :
            if so == sol :
                conn, addr = sol.accept()
                info(('Connected by', addr))
                iwtd.append(conn)
                uv = struct()
                uv.addr = addr
                uv.power = 1
                uv.last_task_time = 0
                uvs[conn] = uv
                info([uv])
            else :
                conn = so
                
                uv = uvs[conn]
                
                # based on how long this task took to complete, adjust
                # UV power rating
                if uv.last_task_time > 0 :
                    if now - uv.last_task_time < min_time_per_task_sec :
                        uv.power = uv.power * 2
                        dbg(('increased power of ', uv.addr, ' to ', uv.power))
                    elif now - uv.last_task_time > max_time_per_task_sec :
                        if uv.power > 1 :
                            uv.power = uv.power / 2
                        
                task_results, dummy = so_read_task(so)
                task_info, task_results = task_results
                dbg(('task_info ', task_info))
                nof_task_result = task_info
                for result in task_results :
                    job_add_result(nof_task_result, result)
                    nof_task_result = nof_task_result + 1
                outstanding_tasks.pop(uv)
      
            # send out another task
            
            # If we have a task that needs to be redone,
            # redo it within UV's constraints. 
            # Otherwise, generate a fresh task
            if len(redo_tasks) > 0 :
                nof_task_base, task_len = redo_tasks[0]
                if task_len > uv.power :
                    redo_tasks[0] = (nof_task_base + uv.power, task_len - uv.power) 
                    task_len = uv.power
                else : # entire redo task is consumed
                    redo_tasks.pop(0)
                dbg(('redoing task ', nof_task_base, ' ', task_len))
                
            else : # this is a fresh task
                nof_task_base = nof_tasks
                task_len = uv.power
                nof_tasks = nof_tasks + task_len # advance fresh task starting point
                
            # accumulate tasks given the task's length
            task_args = []
            for i in range(0, task_len) :
                arg = job_get_arg(nof_task_base + i)
                if arg == '' : break
                task_args.append(arg)
            
            if len(task_args) > 0 :
                task_info = (nof_task_base, task_args)
                # If this UV has already done some work, then it has the task code. 
                # Don't bother sending the task code again.
                if uv.last_task_time > 0 :
                    task = ''
                else :
                    task = job_worker_str + '\nresult = job_worker(arg)\n'
                print len(str(task_args))
                so_write_task(conn, (task_info, task))
                uv.last_task_time = now
                uv.last_task_base = nof_task_base
                outstanding_tasks[uv] = (nof_task_base, uv.power)
            
            dbg(('outstanding ', len(outstanding_tasks)))

            if len(outstanding_tasks) == 0 :
             done = 1
             break

        except (socket.error, ValueError) :
            iwtd.remove(conn)
            uv = uvs[conn]
            # If this UV is currently running a task
            # remove task from pending tasks
            # and put in the redo list
            if outstanding_tasks.has_key(uv) :
                dbg(('queueing task for redo ', outstanding_tasks[uv]))
                redo_tasks.append(outstanding_tasks[uv])
                outstanding_tasks.pop(uv)
                
            uvs.pop(conn, 0)
            

sol.close()

job_finish()
