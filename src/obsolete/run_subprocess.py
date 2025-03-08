import subprocess

# Run the script with unbuffered output
process = subprocess.Popen(
    ["python", "-u", r"C:\Users\ekait\Git\DSC288R_Capstone\src\ml_processing\your_script.py"],  # The "-u" flag ensures Python runs in unbuffered mode
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    bufsize=1  # Line-buffered mode
)

# Stream output in real time
while process.poll() is None:  # While process is running
    line = process.stdout.readline()
    if line:
        print("STDOUT:", line.strip())

    err_line = process.stderr.readline()
    if err_line:
        print("STDERR:", err_line.strip())

# Ensure remaining output is captured
for line in process.stdout:
    print("STDOUT:", line.strip())

for err_line in process.stderr:
    print("STDERR:", err_line.strip())

process.wait()
