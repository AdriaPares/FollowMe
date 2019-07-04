html_layout = '''<!DOCTYPE html>
                    <html>
                        <head>
                            {%metas%}
                            <title>{%title%}</title>
                            {%favicon%}
                            {%css%}
                        </head>
                        <body>
                            <nav>
                              <a href="/"><i class="fas fa-home"></i> FollowMe</a>
                              <a href="/followers/"><i class="fas fa-chart-line"></i> Follower Count</a>
                              <a href="/categories/"><i class="fas fa-medal"></i> Categories</a>
                            </nav>
                            {%app_entry%}
                            <footer>
                                {%config%}
                                {%scripts%}
                                {%renderer%}
                            </footer>
                        </body>
                    </html>'''
