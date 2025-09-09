"script to produce the GB storage chart."

import io

import pandas as pd
import plotly.graph_objects as go
import requests
from plotly.subplots import make_subplots


class GBGasStorage:
    "class that ingests NG storage data and plots based on type of storage."

    def __init__(self) -> None:
        "Initiates the class."
        # Dict mapping the metric names to just the locations.
        self.mapping_dict = {
            "Available Capacity, Aldbrough, Medium Range Storage": "Aldbrough",
            "Available Capacity, Dragon, LNG Importation": "Dragon",
            "Available Capacity, Hatfield Moor, Medium Range Storage": "Hatfield Moor",
            "Available Capacity, Hill Top, Medium Range Storage": "Hill Top",
            "Available Capacity, Holehouse Farm, Medium Range Storage": "Holehouse Farm",
            "Available Capacity, Holford, Medium Range Storage": "Holford",
            "Available Capacity, Humbly Grove, Medium Range Storage": "Humbly Grove",
            "Available Capacity, Hornsea, Medium Range Storage": "Hornsea",
            "Available Capacity, Isle Of Grain, LNG Importation": "Isle Of Grain",
            "Available Capacity, Rough, Long Range Storage": "Rough",
            "Available Capacity, Stublach, Medium Range Storage": "Stublach",
            "Available Capacity, South Hook, LNG Importation": "South Hook",
            "Opening Stock, Aldbrough, Medium Range Storage": "Aldbrough",
            "Opening Stock, Dragon, LNG Importation": "Dragon",
            "Opening Stock, Hatfield Moor, Medium Range Storage": "Hatfield Moor",
            "Opening Stock, Hill Top, Medium Range Storage": "Hill Top",
            "Opening Stock, Holehouse Farm, Medium Range Storage": "Holehouse Farm",
            "Opening Stock, Holford, Medium Range Storage": "Holford",
            "Opening Stock, Humbly Grove, Medium Range Storage": "Humbly Grove",
            "Opening Stock, Hornsea, Medium Range Storage": "Hornsea",
            "Opening Stock, Isle Of Grain, LNG Importation": "Isle Of Grain",
            "Opening Stock, Rough, Long Range Storage": "Rough",
            "Opening Stock, Stublach, Medium Range Storage": "Stublach",
            "Opening Stock, South Hook, LNG Importation": "South Hook",
        }

        self.ng_data = self.clean_data(self.get_data())
        self.storage_data = self.split_data()
        self.fig = self.make_fig()

    def get_data(self) -> dict[str, str | pd.DataFrame]:
        """Retrieved the National Gas storage data from their API, data has to be collected from each storage facility.

        Returns:
            dict[str, str | pd.DataFrame]: dict collection the data on the two metrics collected for each location.
        """
        today = pd.Timestamp.now()
        today = today.strftime("%Y-%m-%d")
        start_day = pd.Timestamp.now() - pd.DateOffset(years=2)
        start_day = start_day.strftime("%Y-%m-%d")

        ng_data = {
            "Opening Stock": {
                "url": f"https://data.nationalgas.com/api/find-gas-data-download?applicableFor=Y&dateFrom={start_day}&dateTo={today}&dateType=GASDAY&latestFlag=N&ids=PUBOBJ2367,PUBOBJ2372,PUBOBJ2365,PUBOBJ2369,PUBOBJ2366,PUBOBJ2368,PUBOBJ2362,PUBOBJ2361,PUBOBJ2363,PUBOBJ2364,PUBOBJ2371,PUBOBJ2370&type=CSV",
                "df": "placeholder",
            },
            "Available Capacity": {
                "url": f"https://data.nationalgas.com/api/find-gas-data-download?applicableFor=Y&dateFrom={start_day}&dateTo={today}&dateType=GASDAY&latestFlag=N&ids=PUBOBJ2431,PUBOBJ2436,PUBOBJ2429,PUBOBJ2433,PUBOBJ2430,PUBOBJ2432,PUBOBJ2426,PUBOBJ2425,PUBOBJ2427,PUBOBJ2428,PUBOBJ2435,PUBOBJ2434&type=CSV",
                "df": "placeholder",
            },
        }
        for value in ng_data.values():
            response = requests.get(value["url"], timeout=12).content
            value["df"] = pd.read_csv(io.StringIO(response.decode("utf-8")))
        return ng_data

    def clean_data(
        self, ng_data: dict[str, str | pd.DataFrame]
    ) -> dict[str, str | pd.DataFrame]:
        """Cleans retrieved gas storage data, by converting datatypes and removing duplicates/unnecessary columns.

        Args:
            ng_data (dict[str, str  |  pd.DataFrame]): gas data dict, broken down by metric.

        Returns:
            dict[str, str | pd.DataFrame]: dictionary containing the cleaned data.
        """
        for trace in ng_data.values():
            temp_df = trace["df"]
            temp_df["Applicable For"] = pd.to_datetime(
                temp_df["Applicable For"], dayfirst=True
            )
            temp_df["Applicable At"] = pd.to_datetime(
                temp_df["Applicable At"], dayfirst=True
            )
            trace["df"] = temp_df.drop_duplicates(
                subset=["Data Item", "Applicable For"], keep="first"
            )
            trace["df"] = trace["df"].drop(
                columns=["Generated Time", "Quality Indicator", "Applicable At"]
            )

        return ng_data

    def split_data(self) -> dict[str, str | pd.DataFrame]:
        """Instead of splitting the data by metric it is split here by type of storage.

        Args:
            ng_data (dict[str, str  |  pd.DataFrame]): cleaned gas data dict split by metric.

        Returns:
            dict[str, str | pd.DataFrame]: cleaned gas data dict split by storage type.
        """
        storage_data = {
            "Long Range Storage": {"df": "placeholder", "row": 1, "col": 1},
            "Medium Range Storage": {"df": "placeholder", "row": 1, "col": 2},
            "LNG Importation": {"df": "placeholder", "row": 1, "col": 3},
        }
        for key, item in storage_data.items():
            combined_data = pd.DataFrame()
            for value in self.ng_data.values():
                value_df = value["df"]
                temp_df = value_df[value_df["Data Item"].str.contains(key)]
                combined_data = pd.concat([combined_data, temp_df], ignore_index=True)
            item["df"] = combined_data

        return storage_data

    def make_fig(self) -> go.Figure:
        """Creates a subplot figure (1x3) of stacked bar charts, displaying the gas storage capacity and availability.

        Args:
            storage_data (dict[str, str | pd.DataFrame | int]): cleaned gas data dict split by storage type.
            ng_data (dict[str, str | pd.DataFrame]): cleaned gas data dict split by metric.

        Returns:
            tuple[go.Figure, dict[str, str|pd.DataFrame|int]]: subplot figure, and gas data dict split by storage type with totals data added.
        """
        fig = make_subplots(
            rows=1,
            cols=3,
            subplot_titles=[
                "<b>Long Range</b>",
                "<b>Medium Range</b>",
                "<b>LNG Importation</b>",
            ],
        )
        for key, value in self.storage_data.items():
            storage_df = value["df"]
            for indices, (ng_key, ng_value) in enumerate(self.ng_data.items()):
                ng_value["df"] = storage_df[
                    storage_df["Data Item"].str.contains(ng_key)
                ].copy()
                ng_value["df"]["Data Item"] = ng_value["df"]["Data Item"].map(
                    self.mapping_dict
                )
                ng_df = ng_value["df"]
                for ind, storage in enumerate(ng_df["Data Item"].unique()):
                    hovertemplate = (
                        f"{storage}"
                        + "<br><b>At: </b>%{x|%d %b %y}<br>"
                        + f"<b>{ng_key}: </b>"
                        + "%{y:,.1f} Twh<extra></extra>"
                    )
                    show_legend = (ind == 0) and (key == "Long Range Storage")
                    opacity = (
                        0.075 if ng_key == "Available Capacity" else 1 - (ind * 0.075)
                    )
                    temp_df = ng_df[ng_df["Data Item"] == storage]
                    fig.add_bar(
                        x=temp_df["Applicable For"],
                        y=temp_df["Value"] * 10**-9,
                        name=ng_key,
                        marker_color="#216c8f",
                        opacity=opacity,
                        legendgroup=f"{indices}",
                        showlegend=show_legend,
                        row=value["row"],
                        col=value["col"],
                        hovertemplate=hovertemplate,
                    )
                if ng_key == "Opening Stock":
                    capacity_df = pd.DataFrame(
                        ng_df.groupby(by="Applicable For")["Value"].sum().reset_index()
                    )
                    value["capacity"] = capacity_df
                    fig.add_scatter(
                        x=capacity_df["Applicable For"],
                        y=capacity_df["Value"] * 10**-9,
                        name="Total Stock",
                        line={"color": "#216c8f", "width": 1},
                        showlegend=False,
                        row=value["row"],
                        col=value["col"],
                        hovertemplate="<br><b>At: </b>%{x|%d %b %y}<br>"
                        + "<b>Total Stock: </b>"
                        + "%{y:,.1f} Twh<extra></extra>",
                    )
            total_df = pd.DataFrame(
                storage_df.groupby(by="Applicable For")["Value"].sum().reset_index()
            )
            value["total"] = total_df
            fig.add_scatter(
                x=total_df["Applicable For"],
                y=total_df["Value"] * 10**-9,
                name="Total Capacity",
                line={"color": "#216c8f", "width": 1},
                showlegend=False,
                row=value["row"],
                col=value["col"],
                hovertemplate="<br><b>At: </b>%{x|%d %b %y}<br>"
                + "<b>Total Capacity: </b>"
                + "%{y:,.1f} Twh<extra></extra>",
            )
            fig.add_trace(
                go.Scatter(
                    x=[total_df["Applicable For"].iloc[-1]],
                    y=[(total_df["Value"].iloc[-1] * 10**-9) + 0.5],
                    name="Capacity",
                    mode="text",
                    text="Total Capacity",
                    marker_color="#216c8f",
                    showlegend=False,
                    textposition="bottom left",
                    textfont={"color": "#216c8f", "size": 10},
                ),
                row=value["row"],
                col=value["col"],
            )
        fig.update_layout(
            barmode="stack",
            xaxis={"showgrid": False},
            yaxis={"showgrid": False},
            title="UK: Gas Storage<br><span style='font-size:0.8em;color:gray'>Twh</span>",
        )
        fig.update_xaxes(tickformat="%b %y")
        fig.update_yaxes(matches="y")
        return fig
