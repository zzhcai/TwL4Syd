import matplotlib.pyplot as plt

x = ['1-node 1-core', '1-node 8-core', '2-node 8-core']
y = [299, 59, 47]
time_seq = round(47 - (299 - 47) / 7)
y_seq = [time_seq] * 3

plt.tight_layout()
plt.bar(x, y, label='parallelisable')
plt.bar(x, y_seq, label='non-parallelisable')
for a, b in enumerate(y):
    plt.text(a, b, str(b), ha='center', va='bottom')
for a, b in enumerate(y_seq):
    plt.text(a, b, str(b), ha='center', va='bottom')

plt.xlabel("Resources")
plt.ylabel("Time used (second)")
plt.title("Performance comparison")
plt.legend()
plt.savefig('cf.png')
