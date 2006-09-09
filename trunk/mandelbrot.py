import Image

x = 0.0
y = 0.0
zoom = .01
width = 320
height = 200
dwell =  256 * 16 
base_x = x - (width / 2) * zoom
base_y = y - (height / 2) * zoom

def job_init(args) :
    global x, y, zoom, width, height, dwell
    global base_x, base_y, img
    if len(args) == 6 :
        x, y, zoom, width, height, dwell = args
    base_x = x - (width / 2) * zoom
    base_y = y - (height / 2) * zoom
    img = Image.new('RGB', (width, height))
    return 0

def job_get_globals() :
    global x, y, zoom, width, height, dwell
    globals = (x, y, zoom, width, height, dwell, base_x, base_y)
    return globals
    
def job_get_arg(task_num) : 
    global width, height, base_x, base_y, zoom, dwell   
    if (task_num >= width * height) : return ''
    
    return task_num
    
def job_add_result(task_num, result) :
    global width, height, img
    if (task_num >= width * height) : return
    
    x = task_num % width
    y = task_num / width
    
    img.putpixel((x, y), result)
  
def job_finish() :
    img.save('fractal.png')

def job_worker(arg) :
    import cmath
    global globals
    x, y, zoom, width, height, dwell, base_x, base_y = globals
    task_num = arg

    x = base_x + (task_num % width) * zoom
    y = base_y + (task_num / width) * zoom
    
    c = complex(x, y)
    z = complex(0, 0)
    
    iter = 0
    while iter < dwell :
        z = z * z + c
        if (z.conjugate() * z).real > 4.0 :
            break
        iter = iter + 1
        
    if iter == dwell :
        result = (0, 0, 0)
    else : 
        result = (255 - iter % 256, ((iter >> 4) % 256) * 15, 0)
    
    return result