import numpy as np
import plotly.express as px
import ipywidgets as widgets
from IPython.display import display


def correlation_heatmap(corr_matrix, is_abs):
    if is_abs:
        corr_matrix = corr_matrix.abs()
        color_continuous_scale = "Reds"
        zmin = 0
        title = "Matriz de Correlação (valor absoluto)"
    else:
        color_continuous_scale = "RdBu_r"
        zmin = -1
        title = "Matriz de Correlação"

    fig = px.imshow(
        corr_matrix,
        color_continuous_scale=color_continuous_scale,
        zmin=zmin,
        zmax=1,
        title=title,
        labels=dict(color="Correlação"),
        aspect="equal",
    )

    fig.update_layout(width=1000, height=1000 )

    fig.show()

def mi_bar_plot(df):
    fig = px.bar(
        df.head(len(df)),
        x="feature",
        y="mutual_information",
        title="Features por Mutual Information",
    )

    fig.update_layout(xaxis_tickangle=-45)
    fig.show()


def distribution_explorer(df):

    feature_selector = widgets.Dropdown(
        options=df.select_dtypes(include='number').columns.tolist(),
        description='Feature:',
        layout=widgets.Layout(width='400px')
    )

    plot_type = widgets.Dropdown(
        options=['histograma', 'boxplot'],
        description='Gráfico:',
    )

    scale_type = widgets.Dropdown(
        options=['Normal', 'Log'],
        description='Escala:',
    )

    color_by_label = widgets.Checkbox(
        value=False,
        description='Colorir por label'
    )

    bins_slider = widgets.IntSlider(
        value=25,
        min=5,
        max=50,
        step=5,
        description='bins:',
        layout=widgets.Layout(width='400px')
    )

    def update_visibility(change):
        if plot_type.value == 'histograma':
            bins_slider.layout.visibility = 'visible'
        else:
            bins_slider.layout.visibility = 'hidden'

    plot_type.observe(update_visibility, 'value')

    button = widgets.Button(description="Gerar", button_style='primary')

    output = widgets.Output()

    def on_button_click(b):
        with output:
            output.clear_output(wait=True)
            feature = feature_selector.value
            df_plot = df.copy()

            # transformação log
            title_suffix = ""
            if scale_type.value == 'Log':
                df_plot[feature] = np.log1p(df_plot[feature])
                title_suffix = " (log)"

            # colorir por label
            color = 'label' if color_by_label.value and 'label' in df.columns else None

            # histograma
            if plot_type.value == 'histograma':
                fig = px.histogram(
                    df_plot,
                    x=feature,
                    color=color,
                    title=f'Histograma{title_suffix} - {feature}',
                    barmode='stack' if color else None,
                    opacity=0.7 if color else None,
                    nbins= bins_slider.value
                )

            # boxplot
            else:
                if color:
                    fig = px.box(
                        df_plot,
                        x='label',
                        color=color,
                        y=feature,
                        title=f'Boxplot{title_suffix} - {feature} por label'
                    )
                else:
                    fig = px.box(
                        df_plot,
                        y=feature,
                        title=f'Boxplot{title_suffix} - {feature}'
                    )

            # layout da figura
            fig.update_layout(
                height=500,
                width=1200,
                template='plotly_white'
            )

            fig.show()

    button.on_click(on_button_click)

    # display
    display(feature_selector)
    display(plot_type)
    display(scale_type)
    display(color_by_label)
    display(bins_slider)
    display(button)
    display(output)

    update_visibility(None)
