
import psutil
kill_name = " "

def main():
    for proc in psutil.process_iter():
        if "python" in proc.name():
            try:
                cmdline = " ".join( proc.as_dict().get('cmdline', None) )
                if ( ('dask-worker' in cmdline) or ('dask-scheduler' in cmdline) or ('celery' in cmdline) ):
                    print(f'Killing {cmdline}')
                    proc.kill()
            except Exception: pass

if __name__ == "__main__":
    main()