import inspect
import re
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
# State: (task base, job_globals, [task_args])
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
    

class PosdoException(Exception) : pass

def posdo_parse_job_str(job_str) :
    worker_match = re.compile('def.*job_worker').search(job_str)
    offset = worker_match.start()
    if offset == None : raise PosdoException, "Unable to find 'job_worker'" 
    return job_str[:offset], job_str[offset:]

# For now we validity by compiling it
# At some point we may want to do something more elaborate
def posdo_test_job_str(job_str) :
    try :
        x = compile(job_str, 'test', 'exec')
    except :
        raise PosdoException, 'Invalid job'

def posdo_accept_uv() :
    global sol, iwtd, uvs, uv_q
    conn, addr = sol.accept()
    info(('Connected ', addr))
    iwtd.append(conn)
    uv = struct()
    uv.so = conn
    uv.addr = addr
    uv.power = 1
    uv.last_task_time = 0
    uvs[conn] = uv
    info([uv])
    
    uv_q.append(uv) # add to idle list  
        
def posdo_run_job(job_str, job_args) :
    global min_time_per_task_sec, max_time_per_task_sec
    global uv_q, uvs, outstanding_tasks, redo_tasks, new_task_base
    global sol, iwtd, owtd, ewtd
    
    # task counter
    new_task_base = 0

    # reset some info about our UVs
    for so, uv in uvs.iteritems() :
        uv.last_task_time = 0
        uv.power = 1 
        
    job_control_str, job_worker_str = posdo_parse_job_str(job_str)
    posdo_test_job_str(job_control_str)
    posdo_test_job_str(job_worker_str)

    # compile and execute the control module of the job
    x = compile(job_control_str, 'posdo_control.py', 'exec')
    exec(x, globals())

    # initialize the job
    if job_init(job_args) : raise PosdoException, 'Failed job_init()'

    # Get job specific options (optional)
    try :
        options = job_get_options()
    except (ValueError, NameError):
        info(('using option defaults'))
    options = (1, 1)

    info(('options ', options))
    opt_power_scaling, opt_redo_tasks = options

    # Get job globals
    job_globals = job_get_globals()
    
    done = 0
    while not done :

        now = time.time()
    
        # idle UVs need to work
        for uv in uv_q :
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
                nof_task_base = new_task_base
                task_len = uv.power
                new_task_base = new_task_base + task_len # advance fresh task starting point
                
            # accumulate tasks given the task's length
            task_args = []
            for i in range(0, task_len) :
                arg = job_get_arg(nof_task_base + i)
                if arg == '' : break
                task_args.append(arg)
            
            if len(task_args) > 0 :
                # If this UV has already done some work, then it has the task code and globals. 
                # Don't bother sending the task code and globals again.
                if uv.last_task_time > 0 :
                    task = ''
                    task_globals = ''
                else :
                    task = job_worker_str + '\nresult = job_worker(arg)\n'
                    task_globals = job_globals
                
                task_info = (nof_task_base, task_globals, task_args)
                so_write_task(uv.so, (task_info, task))
                uv.last_task_time = now
                uv.last_task_base = nof_task_base
                outstanding_tasks[uv] = (nof_task_base, uv.power)
                
                uv_q.remove(uv)
            else : 
                # If there are no task_args, then there is nothing left to do.
                break

        dbg(('outstanding ', len(outstanding_tasks)))
        
        # If there are UVs available and no more outstanding jobs, we are done
        # XXX This is an implicit check. May make sense to make it explicit
        if len(uvs) > 0 and len(outstanding_tasks) == 0 : done = 1
            
        ri, ro, rerr = select.select(iwtd, owtd, ewtd, 1)
        
        now = time.time()
    
        for so in ri :
            try :
                if so == sol :
                    posdo_accept_uv()
                else :
                    uv = uvs[so]

                    task_results, dummy = so_read_task(uv.so)
                    task_info, task_results = task_results
                    dbg(('task_info ', task_info))
                    nof_task_result = task_info
                    for result in task_results :
                        job_add_result(nof_task_result, result)
                        nof_task_result = nof_task_result + 1
                    outstanding_tasks.pop(uv)
    
                    uv_q.append(uv) # add to idle list    
                    
                    # based on how long this task took to complete, adjust
                    # UV power rating
                    if opt_power_scaling and uv.last_task_time > 0 :
                        if now - uv.last_task_time < min_time_per_task_sec :
                            uv.power = uv.power * 2
                            dbg(('increased power of ', uv.addr, ' to ', uv.power))
                        elif now - uv.last_task_time > max_time_per_task_sec :
                            if uv.power > 1 :
                                uv.power = uv.power / 2
                                dbg(('decreased power of ', uv.addr, ' to ', uv.power))
          
            except (socket.error, ValueError) :
                iwtd.remove(uv.so)
                uv = uvs[uv.so]
                # If this UV is currently running a task
                # remove task from pending tasks
                # and put in the redo list
                if outstanding_tasks.has_key(uv) :
                    if opt_redo_tasks :
                        dbg(('queueing task for redo ', outstanding_tasks[uv]))
                        redo_tasks.append(outstanding_tasks[uv])
                    outstanding_tasks.pop(uv)
                try :
                    uv_q.remove(uv) # if on idle list, remove
                except : pass    
                info(('Disonnected ', uv.addr))
                uvs.pop(uv.so, 0)
    
    job_finish() # signal job finished

port = long(sys.argv[1])

#job_filename = sys.argv[2]
#job_args = sys.argv[3:]

min_time_per_task_sec = 10
max_time_per_task_sec = 60
new_task_base = 0

uv_q = []  # UV
uvs = {} # so -> UV
outstanding_tasks = {} # elements of the form (task_base, nof_tasks)
redo_tasks = [] # elements of the form (task_base, nofs_tasks)

# select lists
iwtd = []
owtd = []
ewtd = []

# open up our listening socket on the correct IP:port
host = ''
sol = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sol.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
if hasattr(socket, "SO_REUSEPORT") :
    sol.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
sol.bind((host, port))
sol.listen(3)
iwtd.append(sol) # add socket to the select input list

print 'ok'
while 1 :
    ri, ro, rerr = select.select(iwtd + [sys.stdin], owtd, ewtd, 30)
    for so in ri :
        if so == sol :
            posdo_accept_uv()
        elif so == sys.stdin :
            # parse the command
            line = sys.stdin.readline()
            line_list = line.split()
            
            try :
                job_filename = line_list[0]
                job_args = line_list[1:] # XXX wrong parsing for arguments in quotes with spaces
            except :
                print '? Syntax Error'
                continue
            
            try :
                job_file = open(job_filename, 'r')
                job_str = job_file.read()
                job_file.close()
            except IOError, inst :
                print "%s: %s" % (job_filename, inst);
                continue    
            posdo_run_job(job_str, job_args)
            print 'ok'    

sol.close()

