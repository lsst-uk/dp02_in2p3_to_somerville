import pandas as pd
import pandas as np
import matplotlib.pyplot as plt

def human_readable_hours(mins):
    hours = mins // 60
    remaining_minutes = mins % 60
    if hours >0:
        return str(round(hours))+"hr "+str(round(remaining_minutes))+"m"
    else: 
        return str(round(remaining_minutes))+"m" 


def plot_tb_per_hr():
    stats = pd.read_csv('../log/deepCoadd_calexp_transfer_stats.csv')
    stats['total_time_h']=stats['total_time_s']/3600
    stats['total_time_m']=stats['total_time_s']/60
    stats["tb_per_hr"]=stats["data_volume_TB"]/stats['total_time_h']
    stats['total_time_human'] = stats['total_time_m'].apply(lambda x: human_readable_hours(x))
    print(stats)  
    # x=[16,32,64,128]
    # xlabels = ["16","32","64","128"]

    # plt.figure()
    # plt.title("Transfer speed")
    # plt.xticks(x,xlabels)
    # plt.xlabel('max concurrent connections')
    # plt.ylabel('TB/hr')
    # plt.plot(x,stats["tb_per_hr"],marker='*',linestyle='--',linewidth=0.7,markersize=8,label='read SSL socket',color='green')
    # plt.show()

def plot_costly_fx():
    tottime_df = pd.read_csv("../log/deepCoadd_calexp_transfer_stats_tottime.csv")
    x=[16,32,64,128]
    xlabels = ["16","32","64","128"]
    plt.figure()
    plt.xticks(x,xlabels)
    plt.xlabel('max cuncurrent connections')
    plt.ylabel('time (minutes)')


    # <method 'read' of '_ssl._SSLSocket' objects>
    # Reads Data from the Encrypted Channel: When you call the read method on an _SSLSocket object, it reads the encrypted data transmitted over the socket.
    # Decryption: The data read by this method is encrypted as it's transmitted over the network. The _SSLSocket object automatically decrypts the data upon reading it, so you receive the plaintext version.
    # The method returns the decrypted data as a byte string. If the connection has been closed and all data has been read, it returns an empty byte string.


    # y=tottime_df[tottime_df.fname=="<method 'read' of '_ssl._SSLSocket' objects>"][["tottime_16","tottime_32","tottime_64","tottime_128"]]/60
    # y=y.values.flatten()
    # plt.plot(x,y,marker='*',linestyle='--',linewidth=0.7,markersize=7,label='read SSL socket', color='orange')
    
    # # <method 'poll' of 'select.epoll' objects>
    # # Waits for Events: 
    # y2=tottime_df[tottime_df.fname=="<method 'poll' of 'select.epoll' objects>"][["tottime_16","tottime_32","tottime_64","tottime_128"]]/60
    # y2=y2.values.flatten()
    # plt.plot(x,y2,marker='*',linestyle='--',linewidth=0.7,markersize=7,label='monitor SSL socket',color='purple')

    # <method 'join' of 'bytes' objects>
    # join received bytes
    # y3=tottime_df[tottime_df.fname=="<method 'join' of 'bytes' objects>"][["tottime_16","tottime_32","tottime_64","tottime_128"]]/60
    # y3=y3.values.flatten()
    # plt.plot(x,y3,marker='*',linestyle='--',linewidth=0.7,markersize=7,label='join')
    # <method 'recv_into' of '_socket.socket' objects>
    # reads data from the socket directly into a pre-existing buffer, reducing memory allocation overhead. It returns the number of bytes read.

    # y7=tottime_df[tottime_df.fname=="<method 'recv_into' of '_socket.socket' objects>"][["tottime_16","tottime_32","tottime_64","tottime_128"]]/60
    # y7=y7.values.flatten()
    # plt.plot(x,y7,marker='*',linestyle='--',linewidth=0.7,markersize=7,label='recv_into')

    # <method 'write' of '_ssl.MemoryBIO' objects>
    # The write method of _ssl.MemoryBIO objects in Python adds data to a memory buffer used for SSL/TLS communication.	

    y4=tottime_df[tottime_df.fname=="<method 'write' of '_ssl.MemoryBIO' objects>"][["tottime_16","tottime_32","tottime_64","tottime_128"]]/60
    y4=y4.values.flatten()
    plt.plot(x,y4,marker='*',linestyle='--',linewidth=0.7,markersize=7,label='overhead when writing data into memory buffer')

    # data_received
    # The data_received method is invoked when chunks of data from the HTTP response are received. It processes these chunks, which can involve buffering them, parsing HTTP headers, handling content encoding, etc.

    # y5=tottime_df[tottime_df.fname=="data_received"][["tottime_16","tottime_32","tottime_64","tottime_128"]]/60
    # y5=y5.values.flatten()
    # plt.plot(x,y5,marker='*',linestyle='--',linewidth=0.7,markersize=7,label='handling chunks of data when received')

    # # <method 'run' of '_contextvars.Context' objects>	
    # method of _contextvars.Context objects helps manage and preserve the context-specific state across different asynchronous tasks

    y6=tottime_df[tottime_df.fname=="<method 'run' of '_contextvars.Context' objects>"][["tottime_16","tottime_32","tottime_64","tottime_128"]]/60
    y6=y6.values.flatten()
    plt.plot(x,y6,marker='*',linestyle='--',linewidth=0.7,markersize=7,label='overhead from managing context-specific states for asynchronous tasks')

    plt.legend()
    plt.show()









plot_tb_per_hr()
# plot_costly_fx()