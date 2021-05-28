import pickle
from abc import abstractmethod

import orjson
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
    chunk_size = 15

    def __init__(self):
        super().__init__()
        self.fitted = False
        self.min = None
        self.max = None

    @property
    def ready(self):
        return super().ready and self.fitted

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

    def _normalize(self, arr):
        try:
            self.min = self.min if self.min is not None else arr.min(axis=0)
            self.max = self.max if self.max is not None else arr.max(axis=0)
            return numpy.nan_to_num(
                (arr - self.min) / (numpy.array(self.max) - numpy.array(self.min)),
                nan=0.1,
            )
        except Exception as exc:
            raise

    def _transform(self, arr, *, allow_refill, refill_max_part=0.5):
        numrows = arr.shape[0]
        refill_numrows = self.chunk_size - (numrows % self.chunk_size)
        if allow_refill and refill_numrows / self.chunk_size < refill_max_part:
            refill_sample = numpy.median(arr, axis=0)
            refilled_arr = numpy.vstack([arr] + [refill_sample] * refill_numrows)
            sizematched_array = refilled_arr
        else:
            refill_numrows = 0
            truncate_numrows = numrows - (numrows % self.chunk_size)
            sizematched_array = arr[:truncate_numrows]
        return (
            sizematched_array.reshape(-1, self.chunk_size, arr.shape[1]),
            refill_numrows,
        )

    def _fit(self, arr):
        arr_n = self._normalize(arr)
        x_train, refill = self._transform(arr_n, allow_refill=True, refill_max_part=0.2)
        if not self.ready:
            self._create_model(x_train)
        self.model.fit(
            x_train, x_train, epochs=128, batch_size=3, validation_split=0.1, verbose=0
        )
        print(f"Fit done: {x_train.shape}")
        self.fitted = True

    def detect_anomalies(self, arr) -> numpy.ndarray:
        if not self.ready:
            raise RuntimeError("Model is not ready")
        arr_n = self._normalize(arr)
        x, refill = self._transform(arr_n, allow_refill=True, refill_max_part=1.0)
        pred = self.model.predict(x)
        pred_truncated = pred.reshape((-1,))
        if refill:
            pred_truncated = pred_truncated[:-refill]
        print("Predict done")
        return pred_truncated

    def target(self, arr) -> numpy.ndarray:
        arr_n = self._normalize(arr)
        return numpy.mean(arr_n, axis=1)

    def save(self, filename):
        try:
            if filename.endswith("group_0") == (len(self.max) == 6):
                raise ValueError("AAA")
        except Exception as exc:
            raise
        filepath = "/tmp/" + filename + ".h5"
        self.model.save(filepath)
        return orjson.dumps(
            (filepath, self.min, self.max), option=orjson.OPT_SERIALIZE_NUMPY
        )

    def load(self, data):
        filepath, self.min, self.max = orjson.loads(data)
        self.model = load_model(filepath)
        self.fitted = True
