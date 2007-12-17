import code, re, readline, select, socket, sys, threading, time, zlib

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

def dbg_out(lvl, *s):
    global dbg_lvl
    if lvl <= dbg_lvl: print(''.join([str(x) for x in s]))

def dmp(*s): dbg_out(5, *s)
def dbg(*s): dbg_out(4, *s)    
def info(*s): dbg_out(3, *s)
def wrn(*s): dbg_out(2, *s)
def err(*s): dbg_out(1, *s)

def cli_ok(*s): print ''.join([str(x) for x in s])
def cli_err(*s): print ': '.join(('Error', ''.join([str(x) for x in s])))

class struct: 
    def __str__(self):  return str(self.__dict__)
    def __repr__(self): return str(self.__dict__)
    def __len__(self):  return len(self.__dict__)

class PosdoException(Exception): pass

def so_read_line(so):
    s = ''
    t = so.recv(1)
    while (t and t != '\n'):
        s = s + t
        t = so.recv(1)
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
    task_info, task = s.split('\n', 1)
    return eval(task_info), task
def so_write_block(so, r):
    dmp('<=', len(r), r)
    s = zlib.compress(r)
    t = ''.join((str(len(s)), '\n', s))
    so.sendall(t)
def so_write_task(so, s):
    task_info, s = s
    s = str(task_info) + '\n' + s
    so_write_block(so, s)
    
class Job(object):
    def __init__(self, name, path, args):
        self.name = name
        self.path = path
        self.args = args
        self.inst = None
        # runtime info
        self.task_offset = 0
        self.tasks_done = False # all tasks are issued, may not be processed yet
        self.tasks_outstanding = {} # elements of the form (task_base, nof_tasks)
        self.tasks_redo = [] # elements of the form (task_base, nof_tasks)
    def __getattr__(self, name):
        if self.inst: return self.inst.__dict__[name]
    def done(self):
        is_done = self.tasks_done and len(self.tasks_outstanding) == 0
        if is_done:
            self.inst.job_finish() # signal job finished
        return is_done    
    def load(self, posdo):
        try:
            job_str = open(self.path, 'r').read()
        except IOError, inst:
            s = "%s: %s" % (self.name, inst)
            raise PosdoException, s
        self.parse(job_str, posdo)
    def parse(self, job_str, posdo):
        worker_match = re.compile('def.*job_worker').search(job_str)
        try: 
            offset = worker_match.start()
        except:
            raise PosdoException, "%s: Unable to find 'job_worker'" % (self.name)
        self.code_control, self.code_worker = job_str[:offset], job_str[offset:]
        try:
            # For now we check validity by compiling it
            # At some point we may want to do something more elaborate
            x = compile(self.code_control, 'test', 'exec')
            compile(self.code_worker, 'test', 'exec')
        except Exception, inst:
            raise PosdoException, '%s: Invalid job (%s)' % (self.name, inst)
        self.inst = struct()
        exec(x, self.inst.__dict__)
        # setup posdo accessor class
        posdo_accessor = PosdoAccessor(posdo, self.name)
        self.inst.__dict__.update({'posdo': posdo_accessor})
    def init(self):
        err = self.inst.job_init(self.args)
        if err:
            raise PosdoException, '%s: Failed init' % (self.name)
        # Get job specific options (optional)
        try:
            options = self.inst.job_get_options()
            if options == None: raise ValueError
        except Exception:
            dbg('using option defaults')
            options = (1, 1, 0)
        self.opt_power_scaling, self.opt_tasks_redo, self.opt_tasks_outstanding_max = options
        dbg('options ', options)
        # Get job globals
        self.globals = self.inst.job_get_globals()
    def tasks_at_max(self):
        return self.opt_tasks_outstanding_max > 0 and len(self.task_outstanding) >= self.opt_tasks_outstanding_max
    def uv_drop(self, uv):
        if uv in self.tasks_outstanding:
            task_info = self.tasks_outstanding.pop(uv)
            if self.opt_tasks_redo:
                dbg('queueing task for redo ', task_info)
                self.tasks_redo.append(task_info)
            else:
                (task_offset, task_len) = task_info
                for i in xrange(task_offset, task_offset + task_len):
                    self.inst.job_notify_failure(i) # notify the job of the failure
    def uv_result_process(self, uv):
        result_nof, results = uv.result()
        for result in results:
            self.inst.job_add_result(result_nof, result)
            result_nof += 1
        self.tasks_outstanding.pop(uv)
        # based on how long this task took to complete, adjust
        # UV power rating
        if self.opt_power_scaling:
            uv.power_scale()
    def uv_task(self, uv):
        if self.opt_power_scaling:
            uv_power = uv.power
        else:
            uv_power = 1
        # If we have a task that needs to be done,
        # redo it within UV's constraints. 
        # Otherwise, generate a fresh task
        if len(self.tasks_redo) > 0:
            task_offset, task_len = self.tasks_redo[0]
            if task_len > uv_power: # chop up task to UVs size
                self.tasks_redo[0] = (task_offset + uv_power, task_len - uv_power)
                task_len = uv_power
            else: # entire redo task is consumed
                self.tasks_redo.pop(0)
            dbg('redoing task ', task_offset, ' ', task_len)
        else: # this is a fresh task
            # find out if we are allowed to ask for more args
            try:
                job_status = self.inst.job_get_status()
            except Exception:
                job_status = 1, 0
            if job_status[0] == 0: # job complete
                task_len = 0
                self.tasks_done = True
            elif job_status[0] == 1: # job not complete, may be pending more results
                if job_status[1] == 0: # job is humming along
                    task_offset = self.task_offset
                    task_len = uv_power
                    self.task_offset += task_len
                else: # job is pending more results
                    task_len = 0
        # build the tasks args list
        task_args = []
        for i in xrange(task_len):
            arg = self.inst.job_get_arg(task_offset + i)
            if arg == None:
                self.tasks_done = True
                break
            task_args.append(arg)
        if len(task_args) > 0:    
            uv.task(self, task_offset, task_args)
            self.tasks_outstanding[uv] = (task_offset, task_len)
            dbg('outstanding ', len(self.tasks_outstanding))
            return False
        return True    
            
class Uv(struct):
    def __init__(self, so, addr):
        self.addr = addr
        self.power = 1
        self.so = so
        self.task_time_last = 0
        self.task_offset = None
        self.task_job = None
    def drop(self):
        self.so.close()
        job = self.task_job
        if job: job.uv_drop(self)
    def power_scale(self):
        if self.task_time_last > 0:
            if glbl.now - self.task_time_last < glbl.task_params.time_min_sec:
                self.power *= 2
                dbg('increased power of ', self.addr, ' to ', self.power)
            elif glbl.now - self.task_time_last > glbl.task_params.time_max_sec:
                if self.power > 1:
                    self.power /= 2
                    dbg('decreased power of ', self.addr, ' to ', self.power)
    def result(self):
        task_results, dummy = so_read_task(self.so)
        return task_results
    def task(self, job, task_offset, task_args):
        # If this UV has already done some work, then it has the task code and globals. 
        # Don't bother sending the task code and globals again.
        if self.task_job == job:
            task_code = ''
            task_globals = ''
        else:    
            self.task_job = job
            task_code = job.code_worker
            task_globals = job.globals
        task_info = (task_offset, task_globals, task_args)
        so_write_task(self.so, (task_info, task_code))
        self.task_time_last = glbl.now
        self.task_offset = task_offset

class Posdo(struct):
    def __init__(self):
        self.uv_q = [] # UV
        self.uvs = {} # so -> UV
        self.jobs = [] # job
        # select lists
        self.iwtd = []
        self.owtd = []
        self.ewtd = []
    def run(self):
        # setup listen socket
        sol = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sol.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sol.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sol.bind((glbl.network.host, glbl.network.port))
        sol.listen(3)
        self.iwtd.append(sol) # add socket to the select input list
        now = 0
        while not glbl.done:
            while len(glbl.job_q) > 0:
                job = glbl.job_q.pop()
                try:
                    job.load(self)
                    job.init()
                except Exception, inst:
                    cli_err(inst)
                    continue
                self.jobs.append(job)
            jobs_to_remove = []
            for job in self.jobs:
                if job.tasks_at_max(): continue
                for uv in self.uv_q:
                    if job.tasks_at_max(): break
                    if job.uv_task(uv): break # if no more tasks left, then we are done
                    self.uv_q.remove(uv)
                if job.done():
                    jobs_to_remove.append(job)
            for job in jobs_to_remove:
                cli_ok("Job '%s' complete" % (job.name))
                self.jobs.remove(job)
            jobs_to_remove = []
            if glbl.done: continue
            ri, ro, rerr = select.select(self.iwtd, self.owtd, self.ewtd, 1)
            glbl.now = time.time()
            for so in ri:
                try:
                    if so == sol: # process UV connection
                        new_so, addr = so.accept()
                        uv = Uv(new_so, addr)
                        self.uvs[new_so] = uv
                        self.iwtd.append(new_so)
                        info('Connected ', addr)
                    else: # process UV response
                        uv = self.uvs[so]
                        job = uv.task_job
                        job.uv_result_process(uv)
                    self.uv_q.append(uv) # add to idle list
                except (socket.error, ValueError, AttributeError):
                    info('Disconnected ', uv.addr)
                    self.uvs.pop(uv.so, 0)
                    self.iwtd.remove(uv.so)
                    try:
                        self.uv_q.remove(uv) # remove from idle list if there
                    except:
                        pass
                    uv.drop()

class PosdoAccessor(object):        
    def __init__(self, posdo, job_name):
        self._posdo = posdo
        self._job_name = job_name
    def jobs_nof(self):
        return len(self._posdo.jobs)
    def uvs_nof(self):
        return len(self._posdo.uvs)
    def terminate(self):
        glbl.done = 1
    def dbg(self, *s): dbg('%s: ' % (self._job_name), *s)
    def info(self, *s): info('%s: ' % (self._job_name), *s)
    def err(self, *s): err('%s: ' % (self._job_name), *s)
    
glbl = struct()
glbl.done = False
glbl.job_q = []
glbl.now = None
glbl.network = struct()
glbl.network.port = 0
glbl.task_params = struct()
glbl.task_params.time_min_sec = 20
glbl.task_params.time_max_sec = 120

def thread_cli():
    #readline.parse_and_bind("tab: complete")
    while not glbl.done:
        cli_ok('ok')
        line = raw_input()
        glbl.now = time.time()
        if len(line) == 0: continue
        if line == 'exit':
            glbl.done = True
            break
        if line == 'error':
            cli_err('Manual')
            continue
        line_list = line.split()
        try:
            job_name = line_list[0]
            job_filename = job_name + '.py'
            job_args = line_list[1:] # XXX wrong parsing for arguments in quotes with spaces
        except:
            cli_err('Syntax')
            continue
        try:
            job = Job(job_name, job_filename, job_args)
        except (Exception, PosdoException), inst:
            cli_err(inst)
            continue
        glbl.job_q.append(job)
        
def thread_posdo():
    posdo = Posdo()
    posdo.run()

def main():
    glbl.network.port = int(sys.argv[1])
    glbl.network.host = ''
    threading.Thread(target=thread_cli).start()
    threading.Thread(target=thread_posdo).start()

main()
