"""Initialize app."""
from flask import Flask


def create_app():
    """Construct the core application."""
    app = Flask(__name__,
                instance_relative_config=False)
    app.config.from_object('config.Config')

    with app.app_context():

        # Import main Blueprint
        from . import routes
        app.register_blueprint(routes.main_bp)

        # Import Dash application
        from .dash_application import trends
        app = trends.Add_Dash(app)

        # Second import
        from .dash_application import followers
        app = followers.Add_Dash(app)

        # Third import
        from .dash_application import categories
        app = categories.Add_Dash(app)

        # Fourth import
        from .dash_application import comparison
        app = comparison.Add_Dash(app)

        # Fifth import
        from .dash_application import ranking
        app = ranking.Add_Dash(app)

        return app
