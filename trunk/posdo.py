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

def so_write_block(so, s) :
	t = str(len(s)) + '\n' + s
	print '<=', t
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
	print '=>', s, '\n', blk
	return blk


arg = """
arg = %d
"""


code = """
import time

def work(arg) :
 time.sleep(1)
 return 'did work unit %d' % (arg)
 
# execute
result = work(arg)

"""

iwtd = []
owtd = []
ewtd = []
host = ''
port = long(sys.argv[1])
sol = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sol.bind((host, port))
sol.listen(3)
iwtd.append(sol)
i = 0
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
			
			fc = arg % (i) + code
			i = i + 1
			so_write_block(conn, fc)
 		except socket.error :
 			iwtd.remove(conn)
 			
sol.close()

