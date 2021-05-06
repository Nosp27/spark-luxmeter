import click


@click.group()
def main():
    pass


@main.command()
def loader():
    from .loaders.api import app

    app.start()


@main.command()
def hybrid_metrics():
    from .hybrid_metrics.api import app

    app.start()


@main.command()
def anomaly_detection():
    from .anomaly_detection.api import app

    app.start()


if __name__ == "__main__":
    main()
