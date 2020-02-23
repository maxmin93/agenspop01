import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { DashboardComponent } from './dashboard/dashboard.component';
import { WorkspaceComponent } from './workspace/workspace.component';
// import { WorkspaceGardService } from "./services/workspace-gard.service";

const routes: Routes = [
  { path: '', redirectTo: '/workspace', pathMatch: 'full' },
  { path: 'dashboard', component: DashboardComponent },
  { path: 'workspace', component: WorkspaceComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {

}
