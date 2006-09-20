# Hack to magically upgrade UVs
# Given the FIFO behavior of Posdo's UV idle queue, every UV is going to get passed over, upgraded
# and restarted. The first UV to actually respond (given that all of
# them restart and thus don't complete) is the first one that got upgraded. Hence, we can stop
# XXX This assumes UVs are uniform in the network and are all always upgraded uniformally. Otherwise,
# premature termination could occur
import time
new_uv_ver = 0
new_uv_src = ''
nof_uvs_updated = 0

done = 0

def job_get_options() :
    return (0, 0, 10) # no power scaling, no task redo, 10 max outstanding

def job_init(args) :
    global new_uv_src, new_uv_ver
    f = file(args[0])
    new_uv_src = f.read()
    f.close()
    # find 'new_uv_ver' in upgrade file
    spos = new_uv_src.find('uv_ver')
    if spos == -1 :
        print 'failed to find uv_ver'
        return -1
    epos = new_uv_src.find('\n', spos)
    if epos == -1 :
        print 'failed to uv_ver EOL'
        return -1
    exec(new_uv_src[spos:epos])
    new_uv_ver = uv_ver
    print 'Updating to uv_ver =', new_uv_ver
    return 0

def job_get_globals() :
    return [0, 0] # pass count, dummy
    
def job_get_arg(task_num) : 
    global done, nof_uvs_updated
    if done : return ''
    nof_uvs_updated = task_num#nof_uvs_updated + 1
    return (new_uv_ver, new_uv_src)
    
def job_add_result(task_num, result) :
    global done
    done = 1
    
def job_finish() :
    global nof_uvs_updated
    print 'updated =', nof_uvs_updated
    
def job_worker(arg) :
    import os, sys, time
    
    global uv_ver, globals
    new_uv_ver, new_uv_src = arg
    
    # UV version is the same, no change
    if new_uv_ver == uv_ver :
        return 'ok'
            
    # We have a new UV, upgrade
    uv_file_name = sys.argv[0]
    f = file(uv_file_name, 'w')
    f.write(new_uv_src)
    f.close()
    print 'uv_ver =', new_uv_ver
    #print 'going to spawn', sys.executable, 'with', sys.argv
    
    # XXX close UV's existing socket (we are reaching at this point...)
    global so
    so.close()
    
    # spawn the new UV in our place
    os.execv(sys.executable, tuple([sys.executable]) + tuple(sys.argv))
