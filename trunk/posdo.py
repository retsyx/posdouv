import inspect
import select
import socket
import sys

# Format
#
# Clinet->Server
# <connect>
# Server->Client
# Length\n
# State
# Code
# Client->Server
# Length\n
# Result
# <repeat>/<disconnect>

dbg_lvl = 1

def dbg(s) :
        global dbg_lvl
        if dbg_lvl > 3 : print ''.join(s) 

def so_write_block(so, s) :
	t = str(len(s)) + '\n' + s
	dbg(('<=', t))
	so.send(t)

def so_read_block(so) :
	# read length
	s = ''
	t = so.recv(1)
	while (t and t != '\n') :
		s = s + t
		t = so.recv(1)
	try :
		lblk = long(s)
	except :
		lblk = 0
	
	# if nothing to read, quit
	if lblk == 0 : return ''

 	# read payload
	blk = so.recv(lblk)
	dbg(('=>', s, '\n', blk))
	return blk

job_filename = sys.argv[1]
port = long(sys.argv[2])

job_file = open(job_filename, 'r')
x = compile(job_file.read(), job_filename, 'exec')
job_file.close()
exec(x)
#job_worker_str = inspect.getsource(job_worker) 
# find job_worker() and suck all the source starting with it
job_worker_lines = inspect.findsource(job_worker)
job_worker_str = ''.join(job_worker_lines[0][job_worker_lines[1]:])

iwtd = []
owtd = []
ewtd = []
host = ''
sol = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sol.bind((host, port))
sol.listen(3)
iwtd.append(sol)

while 1 :
	ri, ro, re = select.select(iwtd, owtd, ewtd, 1)
	for so in ri :
		try :
			if so == sol :
				conn, addr = sol.accept()
 	   			print 'Connected by', addr
				iwtd.append(conn)			
 			else :
				conn = so
				result = so_read_block(so)
			        job_add_result(result)

                        # send out another task
			arg = job_get_arg()
                        task = 'arg = ' + str(arg) + '\n'
                        task = task + job_worker_str + '\n'
                        task = task + 'result = job_worker(arg)\n'
			so_write_block(conn, task)
 		except socket.error :
 			iwtd.remove(conn)
 			
sol.close()

