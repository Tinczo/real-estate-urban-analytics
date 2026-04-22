import { Component, signal } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { Map } from 'maplibre-gl';
import { NgxMapLibreGLModule } from '@maplibre/ngx-maplibre-gl';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, NgxMapLibreGLModule],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('deweloperka-frontend');
  
  onMapLoad(map: Map) {
    map.addSource('openfreemap', {
      url: 'https://tiles.openfreemap.org/planet',
      type: 'vector',
    });

    map.addLayer(
      {
        'id': '3d-buildings',
        'source': 'openfreemap',
        'source-layer': 'building',
        'type': 'fill-extrusion',
        'minzoom': 10,
        'filter': ['!=', ['get', 'hide_3d'], true],
        'paint': {
          'fill-extrusion-color': [
            'interpolate',
            ['linear'],
            ['get', 'render_height'],
            0,  '#e3f2fd',
            12, '#90caf9',
            25, '#5c6bc0',
            45, '#7e57c2', 
            80, '#4a148c'
          ],
          'fill-extrusion-height': [
            'interpolate',
            ['linear'],
            ['zoom'],
            15, 0,
            16, ['get', 'render_height']
          ],
          'fill-extrusion-base': [
            'case',
            ['>=', ['get', 'zoom'], 16],
            ['get', 'render_min_height'], 0
          ]
        }
      },
    );
  }
}

