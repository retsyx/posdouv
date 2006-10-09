from optparse import OptionParser, make_option
#import optparse
import pickle
import socket
import sys
import time
import random
import zlib

try :
    import psyco
    psyco.full()
except : pass

uv_ver = 1

class struct : pass

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
        part = so.recv(min(lblk - blk_read, 4096))
        if len(part) == 0 : raise Exception, "Socket dead"
        blk.append(part)
        blk_read = blk_read + len(part)

    blk = ''.join(blk)
    s = zlib.decompress(blk)

    dmp(('=>', len(s), '\n', s))
    return s

def so_read_task(so) :
    s = so_read_block(so)
    task_info, task = s.split('\n', 1)
    return eval(task_info), task

def so_write_block(so, r) :
    dmp(('<=', len(r), r))
    s = zlib.compress(r)
    t = ''.join((str(len(s)), '\n', s))
    so.sendall(t)

def so_write_task(so, s) :
    task_info, s = s
    s = str(task_info) + '\n' + s
    so_write_block(so, s)

REGISTRY_FILE = 'uv.reg'

# XXX for now just assume pwd is the right place for the registry file
def reg_save(registry) :
    try :
        pickle.dump(registry, open(REGISTRY_FILE, 'w'))    
    except Exception, inst:
        err(("Failed to save registry file '", REGISTRY_FILE, "': ", inst, "\nRegistry dump follows:"))
        try :        
            err((pickle.dumps(registry)))
        except Exception, inst:
            err(("Failed to dump registry: ", inst))

def reg_load() :
    try :
        return pickle.load(open(REGISTRY_FILE, 'r'))
    except Exception, inst:
        err(("Failed to load registry file '", REGISTRY_FILE, "': ", inst))
        return {}

def uv_run(registry) :
    job_globals = ''
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
                    # execute payload
                    task_inst = struct()
                    exec(task_obj, task_inst.__dict__)
                    task_inst.__dict__.update({'uv_registry': uv_registry}) # registry
                    task_inst.__dict__.update({'uv_ver': uv_ver}) # to support upgrade
                    task_inst.__dict__.update({'so': so}) # XXX to support upgrade
                
                # only evaluate globals if given
                if task_globals != '' :
                    dbg(('assigning new globals'))
                    dmp((task_globals))
                    job_globals = task_globals
                    task_inst.__dict__.update({'job_globals': job_globals})
                
                dmp((task_args))
                
                # Interpret zero length args as signal to exit
                if len(task_args) == 0 : break
                
                task_results = []
                
                for arg in task_args :
                    dmp(('arg = ', arg))
                
                    result = task_inst.job_worker(arg)
                    
                    task_results.append(result)
                
                # return result
                so_write_task(so, ((task_base, task_results), ''))
        except Exception, inst: 
            err(('Exception: ', inst))
            so.close()
    
        # sleep a little before hammering the server
        time.sleep(random.randint(1, 10))
    
    so.close()

option_list = [
make_option('-a', '--addr', type='string', default='localhost', dest='host_addr'),
make_option('-p', '--port', type='int', default=6666, dest='host_port'),
]

parser = OptionParser(option_list=option_list)
(options, args) = parser.parse_args()

host = options.host_addr
port = options.host_port

registry = reg_load()

try :
    uv_run(registry)
except Exception, inst :
    err(('Exception: ', inst))

reg_save(registry)
 
