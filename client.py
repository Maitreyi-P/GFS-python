
def connect_to_master_server(getCommand,a):
    pass

def connect_to_chunk_server(decision,chunks,filename):
    pass

if __name__=="__main__":
    while True:
        getCommand=input()
        a=len(getCommand.split())
        if a==1:
            if(getCommand=="listfiles"):
                connect_to_master_server(getCommand,a)                    

            if(getCommand=="exit"):
                sys.exit()

        if a==2:
            decision, filename=getCommand.split(' ')
            
            if(decision=="upload"):
                chunks=connect_to_master_server(getCommand,a)
                print(chunks)
                if chunks:
                    connect_to_chunk_server(decision,chunks,filename)
                    print("Uploading of file done!!!")
                else: 
                    print("File Present!!!")    
            
            if(decision=="download"):
                chunks=connect_to_master_server(getCommand,a)
                connect_to_chunk_server(decision,chunks,filename)
                print("Downloading of file done!!!")
            
            if(decision=="lease"):
                connect_to_master_server(getCommand,a)    
            
            if(decision=="unlease"):
                connect_to_master_server(getCommand,a)