"""
================================================================================
Author        : Teow Si Hao Nicholas 
Copyright     : DSO National Laboratories
Date          : 26 July 2024
Purpose       : Script sets up a ZeroMQ subscriber to connect to a publisher,
                receives and prints all incoming messages to inspect their 
                structure and content.
================================================================================
Revision History
Version:	Date:			By:		Description
1.0			26-Jun-2024		TSHN	Initial creation	
================================================================================
"""

import zmq

def main():
    try:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect("tcp://localhost:5556")

        # Subscribe to all topics
        socket.setsockopt_string(zmq.SUBSCRIBE, "data/lcc_status_arr/")

        while True:
            try:
                message = socket.recv_string(flags=zmq.NOBLOCK)
                print(f"Received message: {message}")
            except zmq.Again as e:
                # No message received yet
                pass
            except Exception as e:
                print(f"Error receiving message: {e}")

    except Exception as e:
        print(f"Error setting up subscriber: {e}")

if __name__ == "__main__":
    main()
