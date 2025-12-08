from vowpalwabbit import pyvw


class VWOnlineUpdater:
    """
    Vowpal Wabbit wrapper for incremental updates
    """

    def __init__(self, model_path="vw_model.vw"):
        # Create or load VW model
        self.vw = pyvw.Workspace(model=model_path, quiet=True)

    def learn(self, features: dict, target: float):
        """
        Incremental learning
        features: dict of {feature_name: value}
        target: float
        """
        # Convert features to VW format string: "feature1:value1 feature2:value2 ..."
        feat_str = " ".join(f"{k}:{v}" for k, v in features.items())
        self.vw.learn(f"{target} | {feat_str}")

    def predict(self, features: dict) -> float:
        feat_str = " ".join(f"{k}:{v}" for k, v in features.items())
        return self.vw.predict(f"| {feat_str}")

    def save_model(self, path="vw_model.vw"):
        self.vw.save(path)
        self.vw.save(path)
