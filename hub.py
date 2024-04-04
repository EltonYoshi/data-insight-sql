import subprocess

def main():
    processo_codigo1 = subprocess.Popen(['python', '/home/runner/work/data-insight-sql/data-insight-sql/aws-mysql-python/extrairIDs.py'])
    
    processo_codigo2 = subprocess.Popen(['python', '/home/runner/work/data-insight-sql/data-insight-sql/aws-mysql-python/extrairInsights.py'])
    
    processo_codigo1.wait()
    processo_codigo2.wait()

if __name__ == "__main__":
    main()