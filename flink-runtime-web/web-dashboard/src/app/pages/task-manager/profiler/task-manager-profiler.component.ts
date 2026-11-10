/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute } from '@angular/router';
import { forkJoin, merge, of, Subject } from 'rxjs';
import { catchError, startWith, switchMap, takeUntil } from 'rxjs/operators';

import { HumanizeWatermarkToDatetimePipe } from '@flink-runtime-web/components/humanize-watermark.pipe';
import { ProfilingDetail, ProfilingRequest, PythonWorkerInfo } from '@flink-runtime-web/interfaces/job-profiler';
import { StatusService, TaskManagerService } from '@flink-runtime-web/services';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzSpaceModule } from 'ng-zorro-antd/space';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzTooltipModule } from 'ng-zorro-antd/tooltip';

@Component({
  selector: 'flink-task-manager-profiler',
  templateUrl: './task-manager-profiler.component.html',
  styleUrls: ['./task-manager-profiler.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    NzCardModule,
    NzCheckboxModule,
    NzFormModule,
    NzInputNumberModule,
    FormsModule,
    NzButtonModule,
    NzAlertModule,
    NzTableModule,
    CommonModule,
    NzSpaceModule,
    HumanizeWatermarkToDatetimePipe,
    NzSelectModule,
    NzTooltipModule,
    NzIconModule
  ]
})
export class TaskManagerProfilerComponent implements OnInit, OnDestroy {
  private readonly destroy$ = new Subject<void>();
  private readonly refresh$ = new Subject<void>();
  private autoSelectWorker = false;
  target: 'JVM' | 'PYTHON' = 'JVM';
  pythonWorkers: PythonWorkerInfo[] = [];
  workerId: string | null = null;
  workerLoadFailed = false;
  workerUnavailable = false;
  pythonType: 'CPU' | 'MEMORY' = 'CPU';
  attachRiskAccepted = false;

  get memoryAvailable(): boolean {
    return this.pythonWorkers.some(worker => worker.id === this.workerId && worker.memoryAttachAvailable);
  }

  includeIdle = false;
  includeNative = false;
  profilingList: ProfilingDetail[] = [];
  isLoading = true;
  isCreating = false;
  duration = 30;
  selectMode = 'ITIMER';
  isEnabled = false;
  formatterDuration = (value: number): string => `${value} s`;
  parserDuration = (value: string): string => value.replace(' s', '');

  constructor(
    private taskManagerService: TaskManagerService,
    private readonly activatedRoute: ActivatedRoute,
    private readonly statusService: StatusService,
    private message: NzMessageService,
    private cdr: ChangeDetectorRef
  ) {}

  public workerLabel(worker: PythonWorkerInfo): string {
    const operators = worker.operators.map(
      operator =>
        `${operator.operatorName} [${operator.subtaskIndex}] (attempt ${operator.attemptNumber}, job ${operator.jobId.slice(0, 8)})`
    );
    return operators.length > 0 ? `${operators.join(', ')} — PID ${worker.pid}` : `PID ${worker.pid}`;
  }

  public setTarget(target: 'JVM' | 'PYTHON'): void {
    this.target = target;
    this.attachRiskAccepted = false;
    this.profilingList = [];
    this.workerId = null;
    this.pythonWorkers = [];
    this.workerLoadFailed = false;
    this.workerUnavailable = false;
    this.autoSelectWorker = target === 'PYTHON';
    this.refresh$.next();
  }

  public createProfilingInstance(): void {
    if (this.profilingList.length > 0 && this.profilingList[0].status === 'RUNNING') {
      this.message.warning('Please wait for last profiling finished.');
      return;
    }
    let request: ProfilingRequest;
    if (this.target === 'PYTHON') {
      if (!this.workerId) {
        this.message.warning('Select a Python worker to profile.');
        return;
      }
      if (this.pythonType === 'MEMORY') {
        if (!this.memoryAvailable || !this.attachRiskAccepted) {
          this.message.warning('Memray attach requires an available worker and explicit risk confirmation.');
          return;
        }
        request = {
          workerId: this.workerId,
          type: 'MEMORY',
          duration: this.duration,
          includeNative: this.includeNative
        };
        this.attachRiskAccepted = false;
      } else {
        request = {
          workerId: this.workerId,
          type: 'CPU',
          duration: this.duration,
          includeNative: this.includeNative,
          includeIdle: this.includeIdle
        };
      }
    } else {
      request = { mode: this.selectMode, duration: this.duration };
    }
    this.isCreating = true;
    const taskManagerId = this.activatedRoute.parent!.snapshot.params.taskManagerId;
    this.taskManagerService
      .createProfilingInstance(taskManagerId, request)
      .pipe(takeUntil(this.destroy$))
      .subscribe({
        next: () => {
          this.isCreating = false;
          this.refresh$.next();
        },
        error: error => {
          if (error.status === 409) {
            this.message.warning('A profiling session is active in this Flink JVM or the worker is busy.');
          }
          this.isCreating = false;
          this.cdr.markForCheck();
        }
      });
  }

  public ngOnInit(): void {
    const taskManagerId = this.activatedRoute.parent!.snapshot.params.taskManagerId;
    merge(this.statusService.refresh$, this.refresh$)
      .pipe(
        startWith(true),
        switchMap(() => {
          this.isLoading = true;
          this.cdr.markForCheck();
          return forkJoin({
            history: this.taskManagerService.loadProfilingList(
              taskManagerId,
              this.target === 'PYTHON' ? 'python' : 'jvm'
            ),
            workers:
              this.target === 'PYTHON'
                ? this.taskManagerService.loadPythonWorkers(taskManagerId).pipe(
                    catchError(error => {
                      this.workerUnavailable = error.status === 503;
                      return of(null);
                    })
                  )
                : of([])
          });
        }),
        takeUntil(this.destroy$)
      )
      .subscribe({
        next: data => {
          this.profilingList = data.history.profilingList;
          this.workerLoadFailed = data.workers === null;
          this.pythonWorkers = data.workers ?? [];
          if (!this.pythonWorkers.some(worker => worker.id === this.workerId)) {
            this.workerId = null;
          }
          // Auto-select only on entering Python mode; an exited selection must not switch targets.
          if (this.autoSelectWorker && this.pythonWorkers.length > 0) {
            this.workerId = this.pythonWorkers.length === 1 ? this.pythonWorkers[0].id : null;
            this.autoSelectWorker = false;
          }
          this.isLoading = false;
          this.isEnabled = true;
          this.cdr.markForCheck();
        },
        error: () => {
          this.isLoading = false;
          this.destroy$.next();
          this.destroy$.complete();
          this.cdr.markForCheck();
        }
      });
  }

  public downloadProfilingResult(filePath: string): void {
    const taskManagerId = this.activatedRoute.parent!.snapshot.params.taskManagerId;
    this.isLoading = true;
    this.cdr.markForCheck();

    this.taskManagerService.loadProfilingResult(taskManagerId, filePath).subscribe({
      next: data => {
        const anchor = document.createElement('a');
        anchor.href = data.url;
        anchor.download = data.url;
        document.body.appendChild(anchor);
        anchor.click();
      },
      complete: () => {
        this.isLoading = false;
        this.cdr.markForCheck();
      }
    });
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
