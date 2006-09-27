import socket
import sys
import time

try :
    import psyco
    psyco.full()
except : pass

uv_ver = 1

globals = ''

dbg_lvl = 3

def dbg_out(lvl, s) :
    global dbg_lvl
    if lvl <= dbg_lvl : print ''.join([str(x) for x in s])

def dmp(s) : dbg_out(5, s)
def dbg(s) : dbg_out(4, s)    
def info(s) : dbg_out(3, s)
def wrn(s) : dbg_out(2, s)
def err(s) : dbg_out(1, s)

def so_read_line(so) :
    s = ''
    try :
        t = so.recv(1)
        while (t and t != '\n') :
            s = s + t
            t = so.recv(1)
    except :
        return ''
    
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
    while 1 :
        l = so.send(t)
        if l == len(t) : break
        t = t[l:]

def so_write_task(so, s) :
    task_info, s = s
    s = str(task_info) + '\n' + s
    so_write_block(so, s)
 
host = sys.argv[1]
port = long(sys.argv[2])

while 1 :
    try :
        so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        so.connect((host, port))
        while 1 :
            task_info, task = so_read_task(so)
            task_base, task_globals, task_args = task_info
        
            # only compile task code if it is given, otherwise
            # use previous code
            if task != '' :
                dbg(('compiling new task code'))
                task_str = task[:] # keep a copy for reference if needed later
                task_obj = compile(task, 'posdo', 'exec')
            
            # only evaluate globals if given
            if task_globals != '' :
                dbg(('assigning new globals'))
                dmp((task_globals))
                globals = task_globals
                
            dmp((task_args))
            
            # Interpret zero length args as signal to exit
            if len(task_args) == 0 : break
            
            task_results = []
            
            for arg in task_args :
                dmp(('arg = ', arg))
               
                # clear result
                result = ''
                    
                # execute payload
                exec(task_obj)
                
                task_results.append(result)
                
            # return result
            so_write_task(so, ((task_base, task_results), ''))
    except (ValueError, socket.error) :
        so.close()
    print '.'    
    # sleep a little before hammering the server
    time.sleep(10)

so.close()
 
