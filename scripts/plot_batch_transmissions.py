import matplotlib.pyplot as plt
import numpy as np

# Sample data
labels = ['source', 'processing node', 'sink_node']


send_times =  [(1756499369.2957978, 1756499369.477711), (1756499369.47772, 1756499369.6740491), (1756499369.6740572, 1756499369.8732605), (1756499369.8732693, 1756499370.082368), (1756499370.082377, 1756499370.2847412), (1756499370.2847526, 1756499370.5054445), (1756499370.505453, 1756499370.7077358), (1756499370.7077532, 1756499370.918361), (1756499370.91837, 1756499371.1371932), (1756499371.1372027, 1756499371.3491309)]
forward_times =  [(1756499369.2980874, 1756499369.7165093), (1756499369.7165093, 1756499369.929087), (1756499369.929087, 1756499370.1439888), (1756499370.1439888, 1756499370.3467245), (1756499370.3467245, 1756499370.5528007), (1756499370.5528007, 1756499370.7674944), (1756499370.7674944, 1756499370.9725533), (1756499370.9725533, 1756499371.1767592), (1756499371.1767592, 1756499371.3805435), (1756499371.3805435, 1756499371.581119)]
receive_times =  [(1756499369.2976134, 1756499369.7868414), (1756499369.7868414, 1756499370.012161), (1756499370.012161, 1756499370.2022371), (1756499370.2022371, 1756499370.4071815), (1756499370.4071815, 1756499370.638083), (1756499370.638083, 1756499370.8392463), (1756499370.8392463, 1756499371.0464957), (1756499371.0464957, 1756499371.2342489), (1756499371.2342489, 1756499371.439226), (1756499371.439226, 1756499371.6342154)]




batch_times = []
for batch_id in range(len(send_times)):
    batch_time = {"duration": [], "start": []}
    batch_time["duration"].append(send_times[batch_id][1] - send_times[batch_id][0])
    batch_time["start"].append(send_times[batch_id][0] - send_times[0][0])

    batch_time["duration"].append(forward_times[batch_id][1] - forward_times[batch_id][0])
    batch_time["start"].append(forward_times[batch_id][0] - send_times[0][0])

    batch_time["duration"].append(receive_times[batch_id][1] - receive_times[batch_id][0])
    batch_time["start"].append(receive_times[batch_id][0] - send_times[0][0])

    batch_times.append(batch_time)



x = np.arange(len(labels))  # the label locations
height = 0.6  # thickness of horizontal bars

# Stacked horizontal bars
for i, batch in enumerate(batch_times):
    #plt.barh(x, send_time[1] - send_time[0], height, left=send_time[0], label=f'batch {i}', hatch='/')

    plt.barh(x, batch["duration"], height, left=batch["start"], label=f'batch {i}', hatch='/')
    # plt.barh(x, execution_time, height, left=optimization_time, label='execution time', hatch='')
    # plt.barh(x, result_transmission, height, left=exec_opt_time, label='result transmission', hatch='|')

# Add labels, title, and custom y-axis tick labels
plt.xlabel('processing time (s)')
plt.yticks(x, labels)
plt.legend()

plt.grid(axis='x')

plt.show()