import {Component, OnInit, Input, Output, EventEmitter} from '@angular/core';
import { IElement } from 'src/app/models/agens-graph-types';
// import {NodeInfo} from "../../models/data-model";
import {PALETTE_DARK, PALETTE_ICON} from 'src/app/utils/palette-colors';
import { IEvent } from 'src/app/models/agens-data-types';

@Component({
  selector: 'app-property',
  templateUrl: './property.component.html',
  styleUrls: ['./property.component.css']
})
export class PropertyComponent implements OnInit {

  palette:string[] = PALETTE_DARK;
  icons:any[] = PALETTE_ICON;

  currIcon:any = undefined;
  currColor:string = '#DCDCDC';   // gainsboro
  @Output() changeStyleEmitter= new EventEmitter<IEvent>();

  //public panelInfo: any = {}; // NodeInfo = new NodeInfo();
  togglePanel:boolean = false;

  target:any = undefined;
  data:any = undefined;
  features:any = undefined;

  currMode:string;
  canPopover:boolean = false;
  @Input() set screenMode(mode:string) {
    this.currMode = mode;
    this.canPopover = (mode == 'canvas');
  }

  constructor() { }

  ngOnInit() {
  }

  /////////////////////////////////////////////////

	public showPanel(e:IElement) {
    this.target = e;
    this.canPopover = (this.currMode == 'canvas' && e.group == 'nodes');

    this.data = e.data;
    this.features = Object.entries(e.scratch).map(([k,v]) => ({key: k, value: v}))
            .filter(x=> (<string>x.key).startsWith('_$$'));
            // .map(x=> x.key = x.key.substr(3));
    this.currColor = e.scratch.hasOwnProperty('_color') ? e.scratch['_color'] : '#DCDCDC';
    this.currIcon = e.scratch.hasOwnProperty('_icon') ? e.scratch['_icon'] : undefined;
    this.togglePanel = true;
	}

	public hidePanel() {
    this.togglePanel = false;
    this.data = undefined;
    this.features = undefined;
    // reset style
    this.currIcon = undefined;
    this.currColor = '#DCDCDC';
  }

  /////////////////////////////////////////////////

  selectColor(value){
    this.currColor = value;
    this.changeStyleEmitter.emit(<IEvent>{ type: 'color', data: {target: this.target, color: value} });
  }

  selectIcon(value){
    if( value.name == 'ban' ) this.currIcon = null;
    else this.currIcon = value;
    this.changeStyleEmitter.emit(<IEvent>{ type: 'icon', data: {target: this.target, icon: this.currIcon} });
  }
}