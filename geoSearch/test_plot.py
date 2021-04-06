#%%
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import seaborn as sns

#%matplotlib inline

x = np.linspace(0, 3*np.pi, 500)
plt.plot(x, np.sin(x**2))
plt.title('A simple chirp');



x = np.linspace(0, 20, 100)
plt.plot(x, np.sin(x))
plt.savefig("../mygraph.png")
"""
plt.show() 

sns.set_theme(style="ticks")

df = sns.load_dataset("penguins")
sns.pairplot(df, hue="species").savefig("example.png")
"""
