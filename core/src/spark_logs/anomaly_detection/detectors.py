import pickle
from abc import abstractmethod
from keras.models import load_model

import numpy
from keras import layers, optimizers, Sequential
from sklearn.ensemble import IsolationForest


class Model:
    model_name = None
    support_iterative_fit = False

    def __init__(self):
        self.model = None

    @property
    def ready(self):
        return self.model is not None

    @abstractmethod
    def detect_anomalies(self, arr):
        pass

    def fit(self, arr):
        if not self.support_iterative_fit and self.ready:
            raise RuntimeError("Model does not support iterative fit")
        self._fit(arr)

    @abstractmethod
    def _fit(self, arr):
        pass

    @abstractmethod
    def save(self, filename) -> str:
        pass

    @abstractmethod
    def load(self, filepath):
        pass


class IForestDetector(Model):
    model_name = "iforest"

    def _fit(self, data):
        model = IsolationForest(max_features=3)
        model.fit(data)
        self.model = model

    def detect_anomalies(self, arr) -> numpy.array:
        return self.model.predict(arr)

    def save(self, filename):
        filepath = "/tmp/" + filename
        pickle.dump(self.model, open(filepath, "wb"))
        return filepath

    def load(self, filepath):
        self.model = pickle.load(self.model, open(filepath, "rb"))


class AutoencoderDetector(Model):
    model_name = "autoencoder"
    support_iterative_fit = True
    chunk_size = 3

    def _create_model(self, arr):
        model = Sequential(
            [
                layers.Input(shape=(arr.shape[1], arr.shape[2])),
                layers.Conv1D(
                    filters=32,
                    kernel_size=7,
                    padding="same",
                    strides=1,
                    activation="relu",
                ),
                layers.Dropout(rate=0.2),
                layers.Conv1D(
                    filters=16,
                    kernel_size=7,
                    padding="same",
                    strides=1,
                    activation="relu",
                ),
                layers.Conv1DTranspose(
                    filters=16,
                    kernel_size=7,
                    padding="same",
                    strides=1,
                    activation="relu",
                ),
                layers.Dropout(rate=0.2),
                layers.Conv1DTranspose(
                    filters=32,
                    kernel_size=7,
                    padding="same",
                    strides=1,
                    activation="relu",
                ),
                layers.Conv1DTranspose(filters=1, kernel_size=7, padding="same"),
            ]
        )
        model.compile(optimizer=optimizers.Adam(learning_rate=0.002), loss="mse")
        self.model = model

    def _fit(self, arr):
        numrows = arr.shape[0]
        truncated_numrows = (numrows - (numrows % self.chunk_size))
        if numrows < self.chunk_size:
            raise RuntimeError("Too small dataset")
        x_train = arr[:truncated_numrows].reshape(-1, self.chunk_size, 1)
        if not self.ready:
            self._create_model(x_train)
        self.model.fit(
            x_train, x_train, epochs=128, batch_size=3, validation_split=0.1,
        )
        print(f"Fit done: {x_train.shape}")

    def detect_anomalies(self, arr) -> numpy.array:
        return self.model.predict(arr)

    def save(self, filename):
        filepath = "/tmp/" + filename + ".h5"
        self.model.save(filepath)
        return filepath

    def load(self, filepath):
        self.model = load_model(filepath)
