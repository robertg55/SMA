def show_graph(dict_data):
    import numpy as np
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import Axes3D

    X = [a / 10 for a in range(-100, 100)]
    Y = [a / 10 for a in range(-100, 100)]
    Z = [0 for a in range(-100, 100) for b in range(-100, 100)]
    for key, value in dict_data.items():
        b, s = key.split("_")
        p = value
        xm = int(round((float(b) + 10) * 10, 0))
        ym = int(round((float(s) + 10) * 10, 0))
        Z[xm * ym] = p

    fig = plt.figure()
    ax = fig.add_subplot(111, projection="3d")

    x, y = np.meshgrid(X, Y)
    ax.plot_surface(x, y, np.array(Z).reshape(len(X), len(Y)))
    plt.show()


def gen_csv(dict_data):
    data = []
    for y in range(-100, 100):
        data.append(dict())
    for key, value in dict_data.items():
        b, s = key.split("_")
        p = value
        d = data[int(round((float(b) + 10) * 10, 0))]
        d.update({float(s): p})

    import csv

    keys = sorted(data[0].keys())

    with open("graph.csv", "w", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
