# Databricks notebook source
# MAGIC %pip install keras
# MAGIC import keras

# COMMAND ----------

model = keras.models.Sequential([keras.layers.Dense(1, input_shape=[1])])
model.compile(optimizer='sgd', loss='mean_squared_error')

# COMMAND ----------

import numpy as np
xs=np.array([-1.0,0.0,1.0,2.0,3.0,4.0], dtype=float)
ys=np.array([-3.0,-1.0,1.0,3.0,5.0,7.0], dtype=float)
model.fit(xs, ys, epochs=1100)


# COMMAND ----------

print(model.predict([10.0]))
