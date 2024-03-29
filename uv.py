from optparse import OptionParser, make_option
import os, pickle, platform, random, socket, sys, time, traceback, zlib

try:
    import psyco
    psyco.full()
except: pass

uv_ver = 1

class struct: pass

dbg_lvl = 3

def dbg_out(lvl, *s):
    global dbg_lvl
    if lvl <= dbg_lvl: print ''.join([str(x) for x in s])

def dmp(*s): dbg_out(5, *s)
def dbg(*s): dbg_out(4, *s)    
def info(*s): dbg_out(3, *s)
def wrn(*s): dbg_out(2, *s)
def err(*s): dbg_out(1, *s)

def so_read_line(so):
    s = ''
    try:
        t = so.recv(1)
        while (t and t != '\n'):
            s = s + t
            t = so.recv(1)
    except:
        return ''
    return s
def so_read_block(so):
    # read length
    s = so_read_line(so)
    try:
        lblk = long(s)
    except:
        lblk = 0
    # if nothing to read, quit
    if lblk == 0: return ''
    # read payload
    blk = []
    blk_read = 0
    while blk_read < lblk:
        part = so.recv(min(lblk - blk_read, 4096))
        if len(part) == 0: raise Exception, "Socket dead"
        blk.append(part)
        blk_read = blk_read + len(part)
    blk = ''.join(blk)
    s = zlib.decompress(blk)
    dmp('=>', len(s), '\n', s)
    return s
def so_read_task(so):
    s = so_read_block(so)
    task_info, task = pickle.loads(s)
    return task_info, task
def so_write_block(so, r):
    dmp('<=', len(r), r)
    s = zlib.compress(r)
    t = ''.join((str(len(s)), '\n', s))
    so.sendall(t)
def so_write_task(so, s):
    task_info, s = s
    s = pickle.dumps((task_info, s))
    so_write_block(so, s)

REGISTRY_SAVE_INTERVAL = 600
REGISTRY_FILE_NAME = 'uv.reg'
REG_UV_ID = 'uv.id'

# XXX for now just assume pwd is the right place for the registry file
def reg_save(registry):
    try:
        pickle.dump(registry, open(REGISTRY_FILE_NAME, 'w'))    
    except Exception, inst:
        err("Failed to save registry file '%s': %s\nRegistry dump follows:" % (REGISTRY_FILE_NAME, inst))
        try:        
            err(pickle.dumps(registry))
        except Exception, inst:
            err("Failed to dump registry: %s" % (inst))
def reg_load():
    try:
        return pickle.load(open(REGISTRY_FILE_NAME, 'r'))
    except Exception, inst:
        err("Failed to load registry file '%s': %s" % (REGISTRY_FILE_NAME, inst))
        return {}

def cpus_nof_detect():
    if platform.system() == 'Darwin': # Mac
           return int(os.popen2("sysctl -n hw.ncpu")[1].read())
    elif platform.system() == 'Windows': # Windows
        if os.environ.has_key("NUMBER_OF_PROCESSORS"):
            cpus_nof = int(os.environ["NUMBER_OF_PROCESSORS"]);
            if cpus_nof > 0:
                return cpus_nof
    elif hasattr(os, "sysconf"): # Unix
       if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
           cpus_nof = int(os.sysconf("SC_NPROCESSORS_ONLN"))
           if cpus_nof > 0:
               return cpus_nof
    # default to 1 if can't find anything useful
    return 1

def uv_run(host, port, uv_registry):
    job_globals = ''
    last_time = 0
    while 1:
        try:
            so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            so.connect((host, port))
            while 1:
                if time.time() - last_time > REGISTRY_SAVE_INTERVAL:
                    reg_save(uv_registry)
                    last_time = time.time()
                task_info, task = so_read_task(so)
                task_base, task_globals, task_args = task_info
                # only compile task code if it is given, otherwise
                # use previous code
                if task != '':
                    dbg('compiling new task code')
                    task_str = task[:] # keep a copy for reference if needed later
                    task_obj = compile(task, 'posdo', 'exec')
                    # execute payload
                    task_inst = struct()
                    exec(task_obj, task_inst.__dict__)
                    task_inst.__dict__.update({'uv_registry': uv_registry}) # registry
                    task_inst.__dict__.update({'uv_ver': uv_ver}) # to support upgrade
                    task_inst.__dict__.update({'so': so}) # XXX to support upgrade
                # only evaluate globals if given
                if task_globals != '':
                    dbg('assigning new globals')
                    dmp(task_globals)
                    job_globals = task_globals
                    task_inst.__dict__.update({'job_globals': job_globals})
                dmp(task_args)
                # Interpret zero length args as signal to exit
                if len(task_args) == 0: break
                task_results = []
                for arg in task_args:
                    dmp('arg = ', arg)
                    result = task_inst.job_worker(arg)
                    task_results.append(result)
                # return result
                so_write_task(so, ((task_base, task_results), ''))
        except Exception, inst: 
            err('Exception: %s' % (inst))
            traceback.print_exc()
            so.close()
        # sleep a little before hammering the server
        time.sleep(random.randint(1, 10))
        if time.time() - last_time > REGISTRY_SAVE_INTERVAL:
            reg_save(uv_registry)
            last_time = time.time()
    so.close()


def main():
    option_list = [
    make_option('-a', '--addr', type='string', default='localhost', dest='host_addr'),
    make_option('-p', '--port', type='int', default=6666, dest='host_port'),
    make_option('-c', '--cpus', type='int', default=None, dest='cpus_nof'),
    ]
    
    parser = OptionParser(option_list=option_list)
    (options, args) = parser.parse_args()
    
    host = options.host_addr
    port = options.host_port

    # XXX registry is shared between all UVs on this machine. This will cause race
    # conditions, especially considering the forking below.
    registry = reg_load()
    if not registry.has_key(REG_UV_ID):
        random.seed(time.time())
        registry[REG_UV_ID] = random.randint(0, 18446462598732840960L)
        reg_save(registry)

    # Get number of CPUs and launch as many UVs
    cpus_nof = options.cpus_nof
    if not cpus_nof:
        cpus_nof = cpus_nof_detect()
    info('Running %d instances' % (cpus_nof))    
    for i in range(cpus_nof-1):
        pid = os.fork()
        if pid == 0: break

    try:
        uv_run(host, port, registry)
    except Exception, inst:
        err('Exception: %s' % (inst))
        traceback.print_exc()
    
    reg_save(registry)
 
main()
