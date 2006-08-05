i = 0

def job_init() :
 global i
 i = 0

def job_get_arg() :
    global i 
    if i == 10000 : return '' 
    arg = str(i)
    i = i + 1
    return arg

def job_add_result(result) :
    print result

def job_finish() :
 pass
       
def job_worker(arg) :
        import time
        global result
        time.sleep(1)
        result = 'performed task %s' % (arg)
        return result

print "job loaded"
