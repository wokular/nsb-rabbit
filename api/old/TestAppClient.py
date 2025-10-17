from AppClient import NSBApplicationClient
import sys

if __name__ == "__main__":
    
    myApp = NSBApplicationClient("myClientId")
    
    
    
    while True:
        # Prompt for user input
        user_input = input("Command: ").strip()
        
        # Split the input to separate command and arguments
        parts = user_input.split()
        
        if not parts:
            print("No command entered. Please try again.")
            continue
        
        # Extract the command and arguments
        command = parts[0]
        args = parts[1:]
        result = None
        
        # Execute based on command with input validation
        if command == "start":
            myApp.start()
        
        elif command == "stop":
            myApp.stop()
        
        elif command == "send":
            if len(args) == 4:
                myApp.send(args[0], args[1], args[2], args[3])
            else:
                print("Error: 'send' command requires 4 arguments (e.g., send id1 id2 somemessage msgid1).")
        
        elif command == "receive":
            if len(args) == 2:
                result = myApp.receive(args[0], args[1])
            elif len(args) == 1:
                result = myApp.receive(args[0])
            else:
                print("Error: 'receive' command requires 2 arguments at minimum (e.g., src id, msg id(optional)).")
        
        elif command == "getMessageState":
            if len(args) == 2:
                result = myApp.getMessageState(args[0], args[1])
            else:
                print("Error: 'getMessageState' command requires 2 argument (e.g., getMessageState client_ip msg_id).")
        
        else:
            print(f"Unknown command: {command}")
            
        print("Result in test:" , result)