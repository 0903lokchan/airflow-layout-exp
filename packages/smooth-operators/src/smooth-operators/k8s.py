from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


class SmoothK8sOperator(KubernetesPodOperator):
    """
    A custom wrapper around the KubernetesPodOperator for common configurations.
    (Implementation details would go here)
    """
    def __init__(self, *args, **kwargs):
        # Default configurations
        default_image = "python:3.12-slim"
        default_namespace = "default"
        default_labels = {"app": "smooth-k8s-operator"}
        
        super().__init__(
            *args,
            image=kwargs.get("image", default_image),
            namespace=kwargs.get("namespace", default_namespace),
            labels=kwargs.get("labels", default_labels),
            **kwargs
        )
