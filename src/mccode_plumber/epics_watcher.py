from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Static
from textual.reactive import reactive
from textual import events

from p4p.client.thread import Context
from p4p.client.thread import Disconnected

import asyncio
import threading


class PVWidget(Static):
    value: reactive[str] = reactive("Connecting...")

    def __init__(self, pvname: str):
        super().__init__()
        self.pvname = pvname
        self.sid = pvname.replace(':', '')
        self.set_class(True, "pv-widget")
        self.value_widget = None  # Store reference directly

    def compose(self) -> ComposeResult:
        yield Static(f"[b]{self.pvname}[/b]", id=f"label-{self.sid}")
        self.value_widget = Static(self.value, id=f"value-{self.sid}")
        yield self.value_widget

    def watch_value(self, value: str):
        if self.value_widget:
            self.value_widget.update(value)

    def update_value(self, new_value: str):
        self.value = new_value

class PVMonitorApp(App):
    CSS = """
    .pv-widget {
        padding: 1 1;
        border: round $primary;
        margin: 1;
    }
    """

    def __init__(self, prefix: str, names: list[str]):
        super().__init__()
        self.pv_widgets = {}
        self.ctx = Context("pva")  # Or 'ca' if using Channel Access
        self.prefix = prefix
        self.names = names
        self.grid = None

    def compose(self) -> ComposeResult:
        from textual.containers import Grid
        self.grid = Grid(id="pv-grid")
        yield self.grid

    def on_mount(self) -> None:
        self.grid.styles.grid_columns = ["1fr", "1fr", "1fr"]  # 3 columns
        self.grid.styles.grid_gap = (1, 1)
        for name in self.names:
            pv = f'{self.prefix}{name}'
            widget = PVWidget(pv)
            self.pv_widgets[pv] = widget
            self.grid.mount(widget)
            threading.Thread(target=self.monitor_pv, args=(pv,), daemon=True).start()

    def monitor_pv(self, pvname: str):
        def callback(value):
            if isinstance(value, Disconnected):
                new_value = "Disconnected"
            else:
                new_value = str(value)
            # Schedule update on the main thread
            asyncio.run_coroutine_threadsafe(
                self.update_widget_value(pvname, new_value),
                self._loop,
            )

        self.ctx.monitor(pvname, callback)

    async def update_widget_value(self, pvname: str, new_value: str):
        widget = self.pv_widgets.get(pvname)
        if widget:
            widget.update_value(new_value)

    async def on_shutdown(self) -> None:
        self.ctx.close()



def get_names_parser():
    from argparse import ArgumentParser
    from mccode_plumber import __version__
    p = ArgumentParser()
    p.add_argument('name', type=str, nargs='+', help='The NTScalar names to watch')
    p.add_argument('-p', '--prefix', type=str, help='The EPICS PV prefix to use', default='mcstas:')
    p.add_argument('-v', '--version', action='version', version=__version__)
    return p


def run_strings():
    args = get_names_parser().parse_args()
    PVMonitorApp(args.prefix, args.name).run()


def get_instr_parser():
    from argparse import ArgumentParser
    from mccode_plumber import __version__
    p = ArgumentParser()
    p.add_argument('instr', type=str, help='The instrument which defines names to watch')
    p.add_argument('-p', '--prefix', type=str, help='The EPICS PV prefix to use', default='mcstas:')
    p.add_argument('-v', '--version', action='version', version=__version__)
    return p

def run_instr():
    from mccode_plumber.manage.orchestrate import get_instr_name_and_parameters
    args = get_instr_parser().parse_args()
    _, parameters = get_instr_name_and_parameters(args.instr)
    names = [p.name for p in parameters]
    PVMonitorApp(args.prefix, names).run()


if __name__ == '__main__':
    run_instr()
