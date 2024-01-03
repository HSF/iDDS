import subprocess

process = subprocess.Popen(['/usr/bin/ps', '-ef'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

# Read the output and error streams asynchronously
output_reader = process.stdout
error_reader = process.stderr

output = ""
error = ""

while True:
    # Read from the output stream
    output_chunk = output_reader.read()
    if output_chunk:
        output += output_chunk
    else:
        break

while True:
    # Read from the error stream
    error_chunk = error_reader.read()
    if error_chunk:
        error += error_chunk
    else:
        break

stdout, stderr = process.communicate()
print("stdout, stderr")
print(stdout)
print(stderr)

output = output + stdout
error = error + stderr
# Wait for the process to finish and get the return code
return_code = process.wait()

# Decode the output and error streams
# output = output.decode('utf-8')
# error = error.decode('utf-8')

# Print the output, error, and return code
print("Output:", output)
print("Error:", error)
print("Return Code:", return_code)
