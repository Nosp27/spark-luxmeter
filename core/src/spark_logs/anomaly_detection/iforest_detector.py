from sklearn.ensemble import IsolationForest


class IForestDetector:
    def __init__(self):
        self.model = self.create_model()

    def create_model(self):
        model = IsolationForest(max_features=3)
        return model

    def detect_anomalies(self, arr):
        return self.model.predict(arr)
