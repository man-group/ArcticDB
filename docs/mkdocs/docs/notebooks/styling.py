# styling.py
try:
    import google.colab
    inColab = True
except ImportError:
    inColab = False

# Define the theme colors (same as in setup_chart_styling)
theme = {
    'header_bg': '#141C52',
    'header_font': '#F9F9F9',
    'cell_bg': '#75D0E8',
    'cell_font': '#141C52',
    'hover_bg': '#783ABB',
    'hover_font': '#F9F9F9',
    'ellipsis_bg': '#DDE5ED',  # Lighter color for ellipsis row
    'ellipsis_font': '#141C52'
}

# Figma color palette
palette_colors = [
    '#783ABB',            # PRIMARY/NEON PURPLE - primary
    '#141C52',            # PRIMARY/DARK BLUE - secondary  
    '#F9F9F9',            # PRIMARY/OFF WHITE - neutral
    '#75D0E8',            # SECONDARY/SKY BLUE - accent
    '#DDE5ED',            # SECONDARY/COOL GREY - light gray
    '#EEF2F6',            # SECONDARY/COOL GREY 50 - lighter gray
    '#43D6AC',            # PRIMARY/NEON GREEN - complementary
    '#A1EBD6',            # PRIMARY/NEON GREEN 50 - lighter green
    '#103392'             # SECONDARY/DEEP BLUE - deeper blue variant
]

def style_table(df, hide_index=True, max_rows=20):
    """
    Applies a custom theme to a Pandas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to style.
        hide_index (bool): Whether to hide the DataFrame index.
        max_rows (int): Maximum number of rows to display.

    Returns:
        Styler object: A styled DataFrame object ready for display.
    """
    # Limit rows first if specified
    if max_rows is not None:
        df = df.head(max_rows)
    
    # Define the styles
    styles = [
        {'selector': 'th', # Style the table headers
         'props': [('background-color', theme['header_bg']),
                   ('color', theme['header_font']),
                   ('font-weight', 'bold'),
                   ('text-align', 'left'),
                   ('padding', '10px')]},
        {'selector': 'td', # Style the data cells
         'props': [('background-color', theme['cell_bg']),
                   ('color', theme['cell_font']),
                   ('padding', '8px')]},
        {'selector': 'tr:hover td', # Style rows on hover
         'props': [('background-color', theme['hover_bg']),
                   ('color', theme['hover_font'])]}
    ]

    # Apply the styles
    styled_df = df.style.set_table_styles(styles)

    if hide_index:
        styled_df = styled_df.hide(axis="index")
        
    # You can add more general formatting here, e.g., for numbers
    # styled_df = styled_df.format('{:,.2f}', subset=df.select_dtypes(include=['number']).columns)

    return styled_df


def style_head_tail_table(df, head_rows=10, tail_rows=10, hide_index=True):
    """
    Creates a styled table showing the first N and last N rows with ellipsis in between.
    
    Args:
        df (pd.DataFrame): The DataFrame to display.
        head_rows (int): Number of rows to show from the beginning.
        tail_rows (int): Number of rows to show from the end.
        hide_index (bool): Whether to hide the DataFrame index.
    
    Returns:
        Styler object: A styled DataFrame object ready for display.
    """
    import pandas as pd
    
    total_rows = len(df)
    
    # If dataframe has fewer rows than head_rows + tail_rows, just style the whole thing
    if total_rows <= head_rows + tail_rows:
        return style_table(df, hide_index=hide_index, max_rows=None)
    
    # Get the first and last rows
    head_df = df.head(head_rows).copy()
    tail_df = df.tail(tail_rows).copy()
    
    # Create ellipsis row
    ellipsis_data = {}
    for col in df.columns:
        ellipsis_data[col] = '...'
    
    # Create a single ellipsis row with the same structure
    ellipsis_df = pd.DataFrame([ellipsis_data], index=['...'])
    
    # Combine head, ellipsis, and tail
    display_df = pd.concat([head_df, ellipsis_df, tail_df], ignore_index=False)
    

    # Define styles with special styling for ellipsis row
    styles = [
        {'selector': 'th',  # Style the table headers
         'props': [('background-color', theme['header_bg']),
                   ('color', theme['header_font']),
                   ('font-weight', 'bold'),
                   ('text-align', 'left'),
                   ('padding', '10px')]},
        {'selector': 'td',  # Style the data cells
         'props': [('background-color', theme['cell_bg']),
                   ('color', theme['cell_font']),
                   ('padding', '8px')]},
        {'selector': 'tr:hover td',  # Style rows on hover
         'props': [('background-color', theme['hover_bg']),
                   ('color', theme['hover_font'])]},
    ]

    # Apply the styles
    styled_df = display_df.style.set_table_styles(styles)
    
    # Apply special styling to the ellipsis row
    def highlight_ellipsis(row):
        if row.name == '...':
            return [f'background-color: {theme["ellipsis_bg"]}; color: {theme["ellipsis_font"]}; text-align: center; font-style: italic;'] * len(row)
        return [''] * len(row)
    
    styled_df = styled_df.apply(highlight_ellipsis, axis=1)

    if hide_index:
        styled_df = styled_df.hide(axis="index")
    
    # Add a caption showing the total size
    styled_df = styled_df.set_caption(f"{total_rows} rows × {len(df.columns)} columns")
    
    return styled_df


def export_table_csv(df, filename):
    """
    Export a DataFrame to CSV format.
    
    Args:
        df: pandas DataFrame (the underlying data, not styled)
        filename: Output filename (should end with .csv)
    """
    df.to_csv("exports/" + filename, index=True)
    print(f"Exported: {filename}")


def export_table(styled_df, base_filename, title="Table Export"):
    """
    Export a styled DataFrame to both HTML and CSV formats in one go.
    
    Args:
        styled_df: Styled DataFrame object from style_table() or style_head_tail_table()
        base_filename: Base filename (without extension) for exports
        title: Title for the HTML page
    
    Returns:
        pandas.DataFrame: The underlying DataFrame that was exported
    """
    if inColab:
        return
    
    # Generate filenames
    csv_filename = f"{base_filename}.csv"
    
    # Extract underlying DataFrame from styled object and export to CSV
    underlying_df = styled_df.data
    export_table_csv(underlying_df, csv_filename)
    
    return underlying_df



# Import plotly for interactive charts
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    import plotly.io as pio
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("Plotly not available. Install with: pip install plotly")





def setup_plotly_theme():
    """
    Set up Plotly with custom styling based on the table theme.
    
    Returns:
        dict: Plotly template and color configuration
    """
    if not PLOTLY_AVAILABLE:
        raise ImportError("Plotly not available. Install with: pip install plotly")
    
    
    # Create custom Plotly template
    custom_template = go.layout.Template(
        layout=go.Layout(
            colorway=palette_colors,
            font=dict(color=theme['header_bg'], size=12),
            plot_bgcolor='white',
            paper_bgcolor='white',
            xaxis=dict(
                gridcolor='#E0E0E0',
                linecolor=theme['header_bg'],
                tickcolor=theme['header_bg'],
                title_font=dict(color=theme['header_bg'], size=14),
                tickfont=dict(color=theme['header_bg'], size=11)
            ),
            yaxis=dict(
                gridcolor='#E0E0E0',
                linecolor=theme['header_bg'],
                tickcolor=theme['header_bg'],
                title_font=dict(color=theme['header_bg'], size=14),
                tickfont=dict(color=theme['header_bg'], size=11)
            ),
            title=dict(
                font=dict(color=theme['header_bg'], size=16),
                x=0.05,
                xanchor='left'
            ),
            legend=dict(
                font=dict(color=theme['header_bg'], size=11),
                bgcolor='rgba(255,255,255,0.8)',
                bordercolor=theme['header_bg'],
                borderwidth=1
            ),
            hoverlabel=dict(
                bgcolor='white',
                bordercolor=theme['header_bg'],
                font=dict(color=theme['header_bg'])
            )
        )
    )
    
    # Register the template
    pio.templates["custom_theme"] = custom_template
    pio.templates.default = "custom_theme"
    
    return {
        'theme': theme,
        'palette': palette_colors,
        'template': custom_template,
        'primary_color': palette_colors[0],
        'secondary_color': palette_colors[1],
        'accent_color': palette_colors[3],
        'text_color': palette_colors[8],
        'light_text': palette_colors[4]
    }


def get_responsive_layout(n_subplots=1, col_wrap=None, mobile_single_column=True):
    """
    Get responsive layout settings that adapt to different screen sizes.
    
    Args:
        n_subplots: Number of subplots in the figure
        col_wrap: Maximum columns in desktop view
        mobile_single_column: Force single column layout on mobile
    
    Returns:
        dict: Layout configuration with responsive settings
    """
    layout_config = {
        'desktop': {
            'margin': dict(t=80, b=80, l=80, r=80),
            'font_size': 12,
            'col_wrap': col_wrap if col_wrap else min(3, n_subplots),
            'spacing': {'horizontal': 0.1, 'vertical': 0.15}
        },
        'tablet': {
            'margin': dict(t=60, b=70, l=60, r=40),
            'font_size': 11,
            'col_wrap': min(2, col_wrap if col_wrap else n_subplots),
            'spacing': {'horizontal': 0.08, 'vertical': 0.12}
        },
        'mobile': {
            'margin': dict(t=40, b=60, l=40, r=20),
            'font_size': 10,
            'col_wrap': 1 if mobile_single_column else min(1, col_wrap if col_wrap else n_subplots),
            'spacing': {'horizontal': 0.05, 'vertical': 0.1}
        }
    }
    
    # Default to desktop settings for initial layout
    return layout_config['desktop']


def create_plotly_catplot(data, x_col, y_col, hue_col=None, col_col=None, title=None, 
                         col_order=None, col_wrap=None, sharey=True, kind="bar", 
                         error_col=None, add_value_labels=False):
    """
    Create an interactive Plotly version of seaborn catplot.
    
    Args:
        data: DataFrame with the data
        x_col: Column for x-axis
        y_col: Column for y-axis values
        hue_col: Column for color grouping
        col_col: Column for faceting (subplots)
        title: Main title for the plot
        col_order: Order of facet columns
        col_wrap: Number of columns before wrapping to next row
        sharey: Whether subplots share y-axis
        kind: Type of plot ("bar", "point", "box", etc.)
        error_col: Column name for error bars
        add_value_labels: Whether to add value labels on bars
    
    Returns:
        plotly.graph_objects.Figure
    """
    if not PLOTLY_AVAILABLE:
        raise ImportError("Plotly not available. Install with: pip install plotly")
    
    # Setup theme
    colors = setup_plotly_theme()
    
    if col_col:
        # Create faceted plot
        unique_cols = data[col_col].unique() if col_order is None else col_order
        n_cols = len(unique_cols)
        
        # Calculate grid dimensions
        if col_wrap is not None:
            n_plot_cols = min(col_wrap, n_cols)
            n_plot_rows = (n_cols + col_wrap - 1) // col_wrap  # Ceiling division
        else:
            n_plot_cols = n_cols
            n_plot_rows = 1
        
        # Get responsive spacing configuration
        responsive_layout = get_responsive_layout(n_cols, col_wrap)
        
        # Create subplots with responsive grid layout
        fig = make_subplots(
            rows=n_plot_rows, cols=n_plot_cols,
            subplot_titles=[str(col) for col in unique_cols],
            shared_yaxes=sharey,
            horizontal_spacing=responsive_layout['spacing']['horizontal'],
            vertical_spacing=responsive_layout['spacing']['vertical']
        )
        
        for i, col_val in enumerate(unique_cols):
            col_data = data[data[col_col] == col_val]
            
            # Calculate row and column for this subplot
            if col_wrap is not None:
                subplot_row = (i // col_wrap) + 1
                subplot_col = (i % col_wrap) + 1
            else:
                subplot_row = 1
                subplot_col = i + 1
            
            if hue_col:
                # Group by hue column
                for j, hue_val in enumerate(col_data[hue_col].unique()):
                    hue_data = col_data[col_data[hue_col] == hue_val]
                    color = colors['palette'][j % len(colors['palette'])]
                    
                    if kind == "bar":
                        bar_kwargs = {
                            'x': hue_data[x_col],
                            'y': hue_data[y_col],
                            'name': str(hue_val),
                            'marker_color': color,
                            'showlegend': (i == 0),  # Only show legend for first subplot
                            'hovertemplate': f"{hue_col}: {hue_val}<br>{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>"
                        }
                        
                        # Add error bars if specified
                        if error_col and error_col in hue_data.columns:
                            bar_kwargs['error_y'] = dict(
                                type='data',
                                array=hue_data[error_col],
                                visible=True,
                                color=colors['accent_color'],
                                thickness=2,
                                width=3
                            )
                        
                        # Add value labels if specified
                        if add_value_labels:
                            bar_kwargs['text'] = hue_data[y_col]
                            bar_kwargs['textposition'] = 'outside'
                            bar_kwargs['texttemplate'] = '%{text:.2f}'
                        
                        fig.add_trace(go.Bar(**bar_kwargs), row=subplot_row, col=subplot_col)
            else:
                # Single series per facet
                if kind == "bar":
                    bar_kwargs = {
                        'x': col_data[x_col],
                        'y': col_data[y_col],
                        'name': str(col_val),
                        'marker_color': colors['palette'][i % len(colors['palette'])],
                        'showlegend': False,
                        'hovertemplate': f"{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>"
                    }
                    
                    # Add error bars if specified
                    if error_col and error_col in col_data.columns:
                        bar_kwargs['error_y'] = dict(
                            type='data',
                            array=col_data[error_col],
                            visible=True,
                            color=colors['accent_color'],
                            thickness=2,
                            width=3
                        )
                    
                    # Add value labels if specified
                    if add_value_labels:
                        bar_kwargs['text'] = col_data[y_col]
                        bar_kwargs['textposition'] = 'outside'
                        bar_kwargs['texttemplate'] = '%{text:.2f}'
                    
                    fig.add_trace(go.Bar(**bar_kwargs), row=subplot_row, col=subplot_col)
    else:
        # Single plot
        fig = go.Figure()
        
        if hue_col:
            # Group by hue column
            for i, hue_val in enumerate(data[hue_col].unique()):
                hue_data = data[data[hue_col] == hue_val]
                color = colors['palette'][i % len(colors['palette'])]
                
                if kind == "bar":
                    fig.add_trace(
                        go.Bar(
                            x=hue_data[x_col],
                            y=hue_data[y_col],
                            name=str(hue_val),
                            marker_color=color,
                            hovertemplate=f"{hue_col}: {hue_val}<br>{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>"
                        )
                    )
        else:
            # Single series
            if kind == "bar":
                fig.add_trace(
                    go.Bar(
                        x=data[x_col],
                        y=data[y_col],
                        marker_color=colors['primary_color'],
                        hovertemplate=f"{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>"
                    )
                )
    
    # Update layout with responsive settings and dynamic height
    if col_col and col_wrap is not None:
        plot_height = max(400, n_plot_rows * 300 + 100)  # Dynamic height based on rows
    else:
        plot_height = None  # Let CSS handle height responsively
    
    # Use responsive layout for margins
    responsive_layout = get_responsive_layout(1 if not col_col else len(data[col_col].unique()), col_wrap)
    
    fig.update_layout(
        title=title,
        height=plot_height,
        margin=responsive_layout['margin'],
        template="custom_theme",
        showlegend=False if col_col else True,  # No legend for faceted plots
        font=dict(size=responsive_layout['font_size']),
        # Add mobile-friendly legend positioning
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.1,
            xanchor="center",
            x=0.5
        ) if not col_col else dict()
    )
    
    # Update axes labels
    fig.update_xaxes(title_text="")
    fig.update_yaxes(title_text="")
    
    return fig


def create_plotly_scatterplot(data, x_col, y_col, hue_col=None, title=None, 
                             show_regression=True, error_bars=None, text_col=None, 
                             xlim=None, ylim=None):
    """
    Create an interactive Plotly scatter plot with optional regression line.
    
    Args:
        data: DataFrame with the data
        x_col: Column for x-axis
        y_col: Column for y-axis
        hue_col: Column for color grouping
        title: Title for the plot
        show_regression: Whether to show regression line
        error_bars: Dict with x/y error bar column names
        text_col: Column for text labels on data points
        xlim: Tuple of (min, max) for x-axis limits
        ylim: Tuple of (min, max) for y-axis limits
    
    Returns:
        plotly.graph_objects.Figure
    """
    if not PLOTLY_AVAILABLE:
        raise ImportError("Plotly not available. Install with: pip install plotly")
    
    # Setup theme
    colors = setup_plotly_theme()
    
    fig = go.Figure()
    
    if hue_col:
        # Group by hue column
        for i, hue_val in enumerate(data[hue_col].unique()):
            hue_data = data[data[hue_col] == hue_val]
            color = colors['palette'][i % len(colors['palette'])]
            
            # Add scatter points
            scatter_kwargs = {
                'x': hue_data[x_col],
                'y': hue_data[y_col],
                'mode': 'markers+text' if text_col else 'markers',
                'name': str(hue_val),
                'marker': dict(color=color, size=8),
                'hovertemplate': f"{hue_col}: {hue_val}<br>{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>"
            }
            
            # Add text labels if specified
            if text_col and text_col in hue_data.columns:
                scatter_kwargs['text'] = hue_data[text_col]
                scatter_kwargs['textposition'] = 'top center'
                scatter_kwargs['textfont'] = dict(color=colors['text_color'], size=9)
            
            # Add error bars if specified
            if error_bars:
                if 'x' in error_bars:
                    scatter_kwargs['error_x'] = dict(type='data', array=hue_data[error_bars['x']], visible=True)
                if 'y' in error_bars:
                    scatter_kwargs['error_y'] = dict(type='data', array=hue_data[error_bars['y']], visible=True)
            
            fig.add_trace(go.Scatter(**scatter_kwargs))
            
            # Add regression line if requested
            if show_regression:
                try:
                    import numpy as np
                    from sklearn.linear_model import LinearRegression
                    
                    # Fit regression
                    X = hue_data[x_col].values.reshape(-1, 1)
                    y = hue_data[y_col].values
                    reg = LinearRegression().fit(X, y)
                    
                    # Create prediction line
                    x_range = np.linspace(hue_data[x_col].min(), hue_data[x_col].max(), 100)
                    y_pred = reg.predict(x_range.reshape(-1, 1))
                    
                    fig.add_trace(go.Scatter(
                        x=x_range,
                        y=y_pred,
                        mode='lines',
                        name=f"{hue_val} trend",
                        line=dict(color=color, width=2, dash='dash'),
                        hovertemplate="Regression line<extra></extra>",
                        showlegend=False
                    ))
                except ImportError:
                    print("sklearn not available for regression lines")
    else:
        # Single series
        scatter_kwargs = {
            'x': data[x_col],
            'y': data[y_col],
            'mode': 'markers+text' if text_col else 'markers',
            'marker': dict(color=colors['primary_color'], size=8),
            'hovertemplate': f"{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>",
            'showlegend': False
        }
        
        # Add text labels if specified
        if text_col and text_col in data.columns:
            scatter_kwargs['text'] = data[text_col]
            scatter_kwargs['textposition'] = 'top center'
            scatter_kwargs['textfont'] = dict(color=colors['text_color'], size=9)
        
        # Add error bars if specified
        if error_bars:
            if 'x' in error_bars:
                scatter_kwargs['error_x'] = dict(type='data', array=data[error_bars['x']], visible=True)
            if 'y' in error_bars:
                scatter_kwargs['error_y'] = dict(type='data', array=data[error_bars['y']], visible=True)
        
        fig.add_trace(go.Scatter(**scatter_kwargs))
        
        # Add regression line if requested
        if show_regression:
            try:
                import numpy as np
                from sklearn.linear_model import LinearRegression
                
                # Fit regression
                X = data[x_col].values.reshape(-1, 1)
                y = data[y_col].values
                reg = LinearRegression().fit(X, y)
                
                # Create prediction line
                x_range = np.linspace(data[x_col].min(), data[x_col].max(), 100)
                y_pred = reg.predict(x_range.reshape(-1, 1))
                
                fig.add_trace(go.Scatter(
                    x=x_range,
                    y=y_pred,
                    mode='lines',
                    line=dict(color=colors['secondary_color'], width=2),
                    hovertemplate="Regression line<extra></extra>",
                    showlegend=False
                ))
            except ImportError:
                print("sklearn not available for regression lines")
    
    # Update layout with responsive settings
    responsive_layout = get_responsive_layout()
    layout_kwargs = {
        'title': title,
        'xaxis_title': x_col,
        'yaxis_title': y_col,
        'height': None,  # Let CSS handle responsive height
        'margin': responsive_layout['margin'],
        'template': "custom_theme",
        'font': dict(size=responsive_layout['font_size']),
        'legend': dict(
            orientation="h",
            yanchor="top",
            y=-0.15,
            xanchor="center",
            x=0.5
        ) if hue_col else dict()
    }
    
    # Set axis limits if specified
    if xlim:
        layout_kwargs['xaxis'] = dict(range=xlim)
    if ylim:
        layout_kwargs['yaxis'] = dict(range=ylim)
    
    fig.update_layout(**layout_kwargs)
    
    return fig


def create_plotly_histplot(data, x_col, hue_col=None, col_col=None, title=None,
                          col_order=None, col_wrap=None, bins=30, adaptive_bins=True,
                          x_title=None, y_title=None):
    """
    Create an interactive Plotly histogram plot with optional faceting.
    
    Args:
        data: DataFrame with the data
        x_col: Column for histogram values
        hue_col: Column for color grouping (not typically used for histograms)
        col_col: Column for faceting (subplots)
        title: Plot title
        col_order: Order for facet columns
        col_wrap: Number of columns to wrap facets
        bins: Number of bins for histogram
        adaptive_bins: Whether to use adaptive binning
        x_title: X-axis title (default: empty string)
        y_title: Y-axis title (default: empty string)
    
    Returns:
        plotly.graph_objects.Figure
    """
    if not PLOTLY_AVAILABLE:
        raise ImportError("Plotly not available. Install with: pip install plotly")
    
    # Setup theme
    colors = setup_plotly_theme()
    
    if col_col:
        # Create faceted histogram plot
        unique_cols = data[col_col].unique() if col_order is None else col_order
        n_cols = len(unique_cols)
        
        # Calculate grid dimensions
        if col_wrap is not None:
            n_plot_cols = min(col_wrap, n_cols)
            n_plot_rows = (n_cols + col_wrap - 1) // col_wrap  # Ceiling division
        else:
            n_plot_cols = n_cols
            n_plot_rows = 1
        
        # Get responsive spacing configuration
        responsive_layout = get_responsive_layout(n_cols, col_wrap)
        
        # Create subplots with responsive grid layout
        fig = make_subplots(
            rows=n_plot_rows, cols=n_plot_cols,
            subplot_titles=[str(col) for col in unique_cols],
            horizontal_spacing=responsive_layout['spacing']['horizontal'],
            vertical_spacing=responsive_layout['spacing']['vertical']
        )
        
        for i, col_val in enumerate(unique_cols):
            col_data = data[data[col_col] == col_val]
            
            # Calculate row and column for this subplot
            if col_wrap is not None:
                subplot_row = (i // col_wrap) + 1
                subplot_col = (i % col_wrap) + 1
            else:
                subplot_row = 1
                subplot_col = i + 1
            
            # Calculate optimal bins for this league if adaptive
            if adaptive_bins:
                data_range = col_data[x_col].max() - col_data[x_col].min()
                if data_range <= 15:  # Low-scoring sports (EPL, NHL)
                    optimal_bins = min(15, int(data_range) + 1)
                elif data_range <= 50:  # Medium-scoring
                    optimal_bins = min(25, int(data_range * 0.5))
                else:  # High-scoring sports
                    optimal_bins = min(30, int(data_range * 0.3))
                league_bins = max(10, optimal_bins)  # Minimum 10 bins
            else:
                league_bins = bins
            
            # Create histogram
            fig.add_trace(
                go.Histogram(
                    x=col_data[x_col],
                    nbinsx=league_bins,
                    name=str(col_val),
                    marker_color=colors['primary_color'],
                    showlegend=False,
                    hovertemplate=f"{col_val}<br>Range: %{{x}}<br>Count: %{{y}}<extra></extra>"
                ),
                row=subplot_row, col=subplot_col
            )
        
        # Update layout with responsive settings and dynamic height
        if col_wrap is not None:
            plot_height = max(400, n_plot_rows * 300 + 100)  # Dynamic height based on rows
        else:
            plot_height = None  # Let CSS handle height responsively
    else:
        # Single histogram
        fig = go.Figure()
        fig.add_trace(
            go.Histogram(
                x=data[x_col],
                nbinsx=bins,
                marker_color=colors['primary_color'],
                hovertemplate=f"Range: %{{x}}<br>Count: %{{y}}<extra></extra>"
            )
        )
        plot_height = None  # Let CSS handle responsive height
    
    # Use responsive layout for margins and fonts
    responsive_layout = get_responsive_layout(1 if not col_col else len(data[col_col].unique()), col_wrap)
    
    # Update layout with responsive settings
    fig.update_layout(
        title=title,
        height=plot_height,
        margin=responsive_layout['margin'],
        template="custom_theme",
        font=dict(size=responsive_layout['font_size']),
        showlegend=False
    )
    
    # Update axes labels (use provided titles or empty strings)
    fig.update_xaxes(title_text=x_title if x_title is not None else "")
    fig.update_yaxes(title_text=y_title if y_title is not None else "")
    
    return fig


def create_plotly_lmplot(data, x_col, y_col, hue_col=None, col_col=None, title=None, 
                        show_regression=True, aspect=1.0):
    """
    Create an interactive Plotly version of seaborn lmplot with faceting.
    
    Args:
        data: DataFrame with the data
        x_col: Column for x-axis
        y_col: Column for y-axis
        hue_col: Column for color grouping
        col_col: Column for faceting (subplots)
        title: Main title for the plot
        show_regression: Whether to show regression lines
        aspect: Aspect ratio for subplots
    
    Returns:
        plotly.graph_objects.Figure
    """
    if not PLOTLY_AVAILABLE:
        raise ImportError("Plotly not available. Install with: pip install plotly")
    
    # Setup theme
    colors = setup_plotly_theme()
    
    if col_col:
        # Create faceted plot
        unique_cols = sorted(data[col_col].unique())
        n_cols = len(unique_cols)
        
        # Get responsive layout and calculate dimensions
        responsive_layout = get_responsive_layout(n_cols, n_cols)
        subplot_height = int(500 * aspect) if aspect else None
        total_width = None  # Let responsive CSS handle width
        
        # Create responsive subplots
        fig = make_subplots(
            rows=1, cols=n_cols,
            subplot_titles=[str(col) for col in unique_cols],
            horizontal_spacing=responsive_layout['spacing']['horizontal']
        )
        
        for i, col_val in enumerate(unique_cols):
            col_data = data[data[col_col] == col_val]
            
            if hue_col:
                # Group by hue column
                for j, hue_val in enumerate(col_data[hue_col].unique()):
                    hue_data = col_data[col_data[hue_col] == hue_val]
                    color = colors['palette'][j % len(colors['palette'])]
                    
                    # Add scatter points
                    fig.add_trace(
                        go.Scatter(
                            x=hue_data[x_col],
                            y=hue_data[y_col],
                            mode='markers',
                            name=str(hue_val),
                            marker=dict(color=color, size=6),
                            showlegend=(i == 0),  # Only show legend for first subplot
                            hovertemplate=f"{hue_col}: {hue_val}<br>{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>"
                        ),
                        row=1, col=i+1
                    )
                    
                    # Add regression line if requested
                    if show_regression:
                        try:
                            import numpy as np
                            from sklearn.linear_model import LinearRegression
                            
                            if len(hue_data) > 1:
                                # Fit regression
                                X = hue_data[x_col].values.reshape(-1, 1)
                                y = hue_data[y_col].values
                                reg = LinearRegression().fit(X, y)
                                
                                # Create prediction line
                                x_range = np.linspace(hue_data[x_col].min(), hue_data[x_col].max(), 50)
                                y_pred = reg.predict(x_range.reshape(-1, 1))
                                
                                fig.add_trace(
                                    go.Scatter(
                                        x=x_range,
                                        y=y_pred,
                                        mode='lines',
                                        line=dict(color=color, width=2),
                                        hovertemplate="Regression line<extra></extra>",
                                        showlegend=False
                                    ),
                                    row=1, col=i+1
                                )
                        except ImportError:
                            print("sklearn not available for regression lines")
            else:
                # Single series per facet
                color = colors['palette'][i % len(colors['palette'])]
                
                fig.add_trace(
                    go.Scatter(
                        x=col_data[x_col],
                        y=col_data[y_col],
                        mode='markers',
                        marker=dict(color=color, size=6),
                        name=str(col_val),
                        showlegend=False,
                        hovertemplate=f"{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>"
                    ),
                    row=1, col=i+1
                )
                
                # Add regression line if requested
                if show_regression:
                    try:
                        import numpy as np
                        from sklearn.linear_model import LinearRegression
                        
                        if len(col_data) > 1:
                            # Fit regression
                            X = col_data[x_col].values.reshape(-1, 1)
                            y = col_data[y_col].values
                            reg = LinearRegression().fit(X, y)
                            
                            # Create prediction line
                            x_range = np.linspace(col_data[x_col].min(), col_data[x_col].max(), 50)
                            y_pred = reg.predict(x_range.reshape(-1, 1))
                            
                            fig.add_trace(
                                go.Scatter(
                                    x=x_range,
                                    y=y_pred,
                                    mode='lines',
                                    line=dict(color='grey', width=2),
                                    hovertemplate="Regression line<extra></extra>",
                                    showlegend=False
                                ),
                                row=1, col=i+1
                            )
                    except ImportError:
                        print("sklearn not available for regression lines")
        
        # Update layout for faceted plot with responsive settings
        fig.update_layout(
            title=title,
            height=subplot_height,
            width=total_width,
            margin=responsive_layout['margin'],
            template="custom_theme",
            font=dict(size=responsive_layout['font_size']),
            legend=dict(
                orientation="h",
                yanchor="top",
                y=-0.15,
                xanchor="center",
                x=0.5
            ) if hue_col else dict()
        )
        
    else:
        # Single plot (non-faceted)
        fig = create_plotly_scatterplot(data, x_col, y_col, hue_col, title, show_regression)
    
    # Update axes labels
    fig.update_xaxes(title_text=x_col)
    fig.update_yaxes(title_text=y_col)
    
    return fig


def export_plot_json(fig, filename, separate_files=False):
    """
    Export a Plotly figure as JSON data compatible with react-plotly.js.
    
    Args:
        fig: Plotly figure object
        filename: Output filename (should end with .json)
        separate_files: If True, exports data/layout/config as separate files
        
    Returns:
        dict: JSON object with data, layout, and config keys
    """
    import json
    import numpy as np
    from datetime import datetime, date
    
    if not PLOTLY_AVAILABLE:
        raise ImportError("Plotly not available. Install with: pip install plotly")
    
    # Custom JSON encoder for numpy types and dates
    class PlotlyJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, (datetime, date)):
                return obj.isoformat()
            return super().default(obj)
    
    # Extract data, layout, and config from the figure
    data = fig.data
    layout = fig.layout
    
    # React-plotly.js compatible config
    config = {
        'displayModeBar': True,
        'displaylogo': False,
        'modeBarButtonsToRemove': ['pan2d', 'lasso2d', 'select2d'],
        'responsive': True,
        'scrollZoom': True,
        'doubleClick': 'reset',
        'showTips': False,
        'toImageButtonOptions': {
            'format': 'png',
            'filename': 'chart',
            'height': 500,
            'width': 700,
            'scale': 1
        }
    }
    
    # Optimize layout for responsive Next.js rendering
    # Convert Plotly layout object to dictionary properly
    layout_dict = layout.to_plotly_json()
    layout_dict.update({
        'autosize': True,
        'margin': {'l': 50, 'r': 50, 't': 50, 'b': 50},
        'paper_bgcolor': 'white',
        'plot_bgcolor': 'white'
    })
    
    # Remove width/height for responsive behavior
    layout_dict.pop('width', None)
    layout_dict.pop('height', None)
    
    # Convert to JSON-serializable format
    # Convert Plotly data traces to JSON format
    data_json = [trace.to_plotly_json() for trace in data]
    
    json_data = {
        'data': data_json,
        'layout': layout_dict,
        'config': config
    }
    
    if separate_files:
        # Export as separate files
        base_name = filename.replace('.json', '')
        
        # Export data
        data_file = f"{base_name}_data.json"
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(json_data['data'], f, indent=2, cls=PlotlyJSONEncoder)
        print(f"Exported data: {data_file}")
        
        # Export layout
        layout_file = f"{base_name}_layout.json"
        with open(layout_file, 'w', encoding='utf-8') as f:
            json.dump(json_data['layout'], f, indent=2, cls=PlotlyJSONEncoder)
        print(f"Exported layout: {layout_file}")
        
        # Export config
        config_file = f"{base_name}_config.json"
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(json_data['config'], f, indent=2)
        print(f"Exported config: {config_file}")
        
    else:
        # Export as single file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, cls=PlotlyJSONEncoder)
        print(f"Exported: {filename}")
    
    


# Example usage in notebook cells:
# # Export single JSON file for React Plotly
# json_data = export_plot_json(fig, "chart_data.json")
# 
# # Export separate data/layout/config files
# export_plot_json(fig, "chart_data.json", separate_files=True)
# 
# # Use in React component:
# import Plot from 'react-plotly.js';
# import chartData from './chart_data.json';
# 
# function MyChart() {
#   return (
#     <Plot
#       data={chartData.data}
#       layout={chartData.layout}
#       config={chartData.config}
#       useResizeHandler={true}
#       style={{width: "100%", height: "100%"}}
#     />
#   );
# }


def export_plot(fig, base_filename, title="Interactive Chart", separate_json_files=False):
    """
    Export a Plotly figure to both HTML and JSON formats in one go.
    
    Args:
        fig: Plotly figure object
        base_filename: Base filename (without extension) for exports
        title: Title for the HTML page
        separate_json_files: If True, exports JSON as separate data/layout/config files
    
    Returns:
        dict: JSON data that was exported
    """
    if inColab:
        return
    
    if not PLOTLY_AVAILABLE:
        raise ImportError("Plotly not available. Install with: pip install plotly")
    
    # Generate filenames
    json_filename = f"exports/{base_filename}.json"
    
    # Export JSON version and return the data
    json_data = export_plot_json(fig, json_filename, separate_files=separate_json_files)
    
    return json_data


def create_plotly_bar_chart(data, x_col, y_col, title=None, error_col=None, 
                           add_value_labels=False):
    """
    Create a simple interactive Plotly bar chart.
    
    Args:
        data: DataFrame with the data
        x_col: Column for x-axis (categories)
        y_col: Column for y-axis (values)
        title: Title for the plot
        error_col: Column name for error bars
        add_value_labels: Whether to add value labels on bars
    
    Returns:
        plotly.graph_objects.Figure
    """
    if not PLOTLY_AVAILABLE:
        raise ImportError("Plotly not available. Install with: pip install plotly")
    
    # Setup theme
    colors = setup_plotly_theme()
    
    # Create bar chart
    fig = go.Figure()
    
    bar_kwargs = {
        'x': data[x_col],
        'y': data[y_col],
        'marker_color': colors['primary_color'],
        'hovertemplate': f"{x_col}: %{{x}}<br>{y_col}: %{{y}}<extra></extra>",
        'text': data[y_col] if add_value_labels else None,
        'textposition': 'outside' if add_value_labels else None,
        'texttemplate': '%{text:.2f}' if add_value_labels else None
    }
    
    # Add error bars if specified
    if error_col and error_col in data.columns:
        bar_kwargs['error_y'] = dict(
            type='data',
            array=data[error_col],
            visible=True,
            color=colors['accent_color'],
            thickness=2,
            width=3
        )
    
    fig.add_trace(go.Bar(**bar_kwargs))
    
    # Update layout with responsive settings
    responsive_layout = get_responsive_layout()
    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        yaxis_title=y_col,
        height=None,  # Let CSS handle responsive height
        margin=responsive_layout['margin'],
        template="custom_theme",
        font=dict(size=responsive_layout['font_size']),
        showlegend=False
    )
    
    return fig


