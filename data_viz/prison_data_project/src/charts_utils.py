import plotly.express as px
import pandas as pd


def create_line_trends_plot(csv_path: str, x_col: str = 'YEAR', y_col: str = 'OCC_ABS', 
                           color_col: str = 'geo', title: str = None, save_path: str = None) -> px.line:
    """
    Generic line plot with markers and customized layout.
    
    Args:
        csv_path: Path to CSV
        x_col, y_col, color_col: columns for axes and color
        title: optional title
        save_path: optional path to save HTML
    
    Returns:
        Plotly Figure
    """
    plot_data = pd.read_csv(csv_path)
    print(f"Data loaded: {len(plot_data)} rows")
    
    fig = px.line(
        plot_data, x=x_col, y=y_col, color=color_col, markers=True,
        labels={y_col: 'Occupancy Rate (%)', x_col: 'Year'},
        title=title
    )
    
    fig.update_layout(
        height=500, width=1000, font_size=14, title_font_size=16,
        legend=dict(
            orientation='h', yanchor='bottom', y=-0.25, 
            xanchor='center', x=0.5, title='Countries'
        ),
        xaxis=dict(showticklabels=True, title=""),  # Hide x-axis title
        xaxis_title=""
    )
    
    if save_path:
        fig.write_html(save_path)
        print(f"💾 HTML saved: {save_path}")
    
    print("✅ Chart object created")
    return fig


def create_simple_bar_plot(csv_path: str, x_col: str, y_col: str, color_col: str = None,
                          title: str = None, save_path: str = None) -> px.bar:
    """
    Simple bar plot (eg. top10 occupancy).
    """
    df = pd.read_csv(csv_path)
    fig = px.bar(df, x=x_col, y=y_col, color=color_col, 
                title=title, text=y_col)
    
    fig.update_traces(textposition='outside')
    fig.update_layout(height=500, width=1000)
    
    if save_path: fig.write_html(save_path)
    return fig


def create_grouped_bar_comparison(csv_path: str, x_col: str = 'REGION', melt_id: str = 'REGION',
                                 melt_vars: list = ['mean', 'min', 'max', 'count'],
                                 title: str = None, save_path: str = None) -> px.bar:
    """
    Grouped bar chart with values on bars.
    """
    df = pd.read_csv(csv_path)
    print(f"📊 Loaded {len(df)} regions: {df[x_col].tolist()}")
    
    df_long = df.melt(id_vars=melt_id, value_vars=melt_vars, 
                     var_name='Metric', value_name='Value')
    df_long['Metric'] = df_long['Metric'].str.title()
    df_long['ValueRounded'] = df_long['Value'].round(1)
    
    fig = px.bar(
        df_long, 
        x=x_col, 
        y='Value', 
        color='Metric', 
        barmode='group',
        text='ValueRounded',
        color_discrete_map={
            'Mean': '#2E86AB', 
            'Min': '#A23B72', 
            'Max': '#F18F01', 
            'Count': '#C73E1D'
        },
        title=title
    )
    
    fig.update_traces(
        texttemplate='%{text}',  # Usa ValueRounded
        textposition='outside',
        textangle=0, 
        textfont_size=12
    )
    
    fig.update_layout(
        height=550, 
        width=1000, 
        yaxis_title="Occupancy Rate (%)",
        legend_title="Metrics"
    )
    
    if save_path:
        fig.write_html(save_path)
        print(f"💾 HTML saved: {save_path}")
    
    return fig
