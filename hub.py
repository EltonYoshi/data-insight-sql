import subprocess

def main():
    processo_codigo1 = subprocess.Popen(['python', '/aws-mysql-python/extrairIDs.py'])
    
    processo_codigo2 = subprocess.Popen(['python', '/aws-mysql-python/extrairInsights.py'])
    
    processo_codigo1.wait()
    processo_codigo2.wait()

if __name__ == "__main__":
    main()