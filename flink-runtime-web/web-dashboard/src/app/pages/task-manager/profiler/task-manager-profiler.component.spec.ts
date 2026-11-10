/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ChangeDetectorRef } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute } from '@angular/router';
import { of, Subject, throwError } from 'rxjs';

import { APP_ICONS } from '@flink-runtime-web/app-icons';
import { ProfilingDetail, PythonWorkerInfo } from '@flink-runtime-web/interfaces/job-profiler';
import { StatusService, TaskManagerService } from '@flink-runtime-web/services';
import { provideNzIcons } from 'ng-zorro-antd/icon';
import { NzMessageService } from 'ng-zorro-antd/message';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { TaskManagerProfilerComponent } from './task-manager-profiler.component';

const cdr = { markForCheck: vi.fn() } as unknown as ChangeDetectorRef;
const activatedRoute = { parent: { snapshot: { params: { taskManagerId: 'tm-1' } } } } as unknown as ActivatedRoute;

describe('TaskManagerProfilerComponent', () => {
  const loadProfilingList = vi.fn();
  const createProfilingInstance = vi.fn();
  const loadPythonWorkers = vi.fn();
  let refresh: Subject<boolean>;
  const firstWorker: PythonWorkerInfo = { id: 'one', pid: 123, memoryAttachAvailable: false, operators: [] };
  const secondWorker: PythonWorkerInfo = { id: 'two', pid: 456, memoryAttachAvailable: false, operators: [] };
  const warning = vi.fn();
  let component: TaskManagerProfilerComponent;

  beforeEach(() => {
    loadProfilingList.mockReset().mockReturnValue(of({ profilingList: [] }));
    createProfilingInstance.mockReset().mockReturnValue(of({ status: 'RUNNING' }));
    loadPythonWorkers.mockReset().mockReturnValue(of([]));
    refresh = new Subject<boolean>();
    warning.mockReset();
    component = new TaskManagerProfilerComponent(
      { loadProfilingList, createProfilingInstance, loadPythonWorkers } as unknown as TaskManagerService,
      activatedRoute,
      { refresh$: refresh } as unknown as StatusService,
      { warning } as unknown as NzMessageService,
      cdr
    );
  });

  afterEach(() => component.ngOnDestroy());

  it('labels a shared worker with both operator instances', () => {
    const worker: PythonWorkerInfo = {
      ...firstWorker,
      operators: [
        { jobId: 'job-123456', operatorId: 'a', operatorName: 'decode', subtaskIndex: 0, attemptNumber: 1 },
        { jobId: 'job-123456', operatorId: 'b', operatorName: 'infer', subtaskIndex: 0, attemptNumber: 1 }
      ]
    };
    expect(component.workerLabel(worker)).toBe(
      'decode [0] (attempt 1, job job-1234), infer [0] (attempt 1, job job-1234) — PID 123'
    );
  });

  it('loads the profiling list for the routed task manager on refresh', () => {
    loadProfilingList.mockReturnValue(of({ profilingList: [{ status: 'FINISHED' } as ProfilingDetail] }));

    component.ngOnInit();

    expect(loadProfilingList).toHaveBeenCalledWith('tm-1', 'jvm');
    expect(component.profilingList).toEqual([{ status: 'FINISHED' }]);
    expect(component.isEnabled).toBe(true);
    expect(component.isLoading).toBe(false);
  });

  it('refreshes the routed task manager history after creating a profiling instance', () => {
    component.ngOnInit();
    loadProfilingList.mockReturnValue(of({ profilingList: [{ status: 'RUNNING' }] }));

    component.createProfilingInstance();

    expect(createProfilingInstance).toHaveBeenCalledWith('tm-1', { mode: 'ITIMER', duration: 30 });
    expect(loadProfilingList).toHaveBeenCalledTimes(2);
    expect(component.profilingList).toEqual([{ status: 'RUNNING' }]);
    expect(component.isCreating).toBe(false);
  });

  it('keeps the selected target history when a creation response arrives after switching targets', () => {
    const pendingCreation = new Subject<ProfilingDetail>();
    createProfilingInstance.mockReturnValue(pendingCreation);
    loadPythonWorkers.mockReturnValue(of([firstWorker]));
    component.ngOnInit();
    component.setTarget('PYTHON');
    component.createProfilingInstance();
    expect(component.isCreating).toBe(true);

    const jvmProfile = { status: 'FINISHED', mode: 'ITIMER' };
    loadProfilingList.mockReturnValue(of({ profilingList: [jvmProfile] }));
    component.setTarget('JVM');
    pendingCreation.next({ status: 'RUNNING', worker: firstWorker } as ProfilingDetail);
    pendingCreation.complete();

    expect(component.target).toBe('JVM');
    expect(component.profilingList).toEqual([jvmProfile]);
    expect(loadProfilingList).toHaveBeenLastCalledWith('tm-1', 'jvm');
    expect(component.isCreating).toBe(false);
  });

  it('does not duplicate a created profile already returned by a refresh', () => {
    const pendingCreation = new Subject<ProfilingDetail>();
    createProfilingInstance.mockReturnValue(pendingCreation);
    component.ngOnInit();
    component.createProfilingInstance();

    const profile = { status: 'RUNNING', mode: 'ITIMER' } as ProfilingDetail;
    loadProfilingList.mockReturnValue(of({ profilingList: [profile] }));
    refresh.next(true);
    pendingCreation.next(profile);
    pendingCreation.complete();

    expect(component.profilingList).toEqual([profile]);
    expect(component.isCreating).toBe(false);
  });

  it('requires explicit risk confirmation and omits idle for memory attach', () => {
    loadPythonWorkers.mockReturnValue(of([{ ...firstWorker, memoryAttachAvailable: true }]));
    component.ngOnInit();
    component.setTarget('PYTHON');
    component.pythonType = 'MEMORY';
    component.includeIdle = true;
    component.createProfilingInstance();
    expect(createProfilingInstance).not.toHaveBeenCalled();
    component.attachRiskAccepted = true;
    component.createProfilingInstance();
    expect(createProfilingInstance).toHaveBeenCalledWith('tm-1', {
      workerId: 'one',
      type: 'MEMORY',
      duration: 30,
      includeNative: false
    });
    expect(component.attachRiskAccepted).toBe(false);
  });

  it('refuses memory capture when the worker has no prepared Memray', () => {
    loadPythonWorkers.mockReturnValue(of([firstWorker]));
    component.ngOnInit();
    component.setTarget('PYTHON');
    component.pythonType = 'MEMORY';
    component.attachRiskAccepted = true;
    component.createProfilingInstance();
    expect(createProfilingInstance).not.toHaveBeenCalled();
  });

  it('refuses to start a new profiling run while one is still running', () => {
    component.profilingList = [{ status: 'RUNNING' } as ProfilingDetail];

    component.createProfilingInstance();

    expect(warning).toHaveBeenCalled();
    expect(createProfilingInstance).not.toHaveBeenCalled();
  });

  it('does not fetch workers while profiling the JVM', () => {
    component.ngOnInit();
    refresh.next(true);
    expect(loadPythonWorkers).not.toHaveBeenCalled();
  });

  it('selects the only worker on entering Python mode and sends sampling options', () => {
    loadPythonWorkers.mockReturnValue(of([firstWorker]));
    component.ngOnInit();
    component.setTarget('PYTHON');
    expect(component.workerId).toBe(firstWorker.id);
    component.includeNative = true;
    component.includeIdle = true;
    component.createProfilingInstance();
    expect(createProfilingInstance).toHaveBeenCalledWith('tm-1', {
      workerId: 'one',
      type: 'CPU',
      duration: 30,
      includeNative: true,
      includeIdle: true
    });
  });

  it('requires a selection when several workers are available', () => {
    loadPythonWorkers.mockReturnValue(of([firstWorker, secondWorker]));
    component.ngOnInit();
    component.setTarget('PYTHON');
    expect(component.workerId).toBeNull();
    component.createProfilingInstance();
    expect(createProfilingInstance).not.toHaveBeenCalled();
    expect(warning).toHaveBeenCalledWith('Select a Python worker to profile.');
    component.workerId = secondWorker.id;
    component.createProfilingInstance();
    expect(createProfilingInstance).toHaveBeenCalledWith('tm-1', {
      workerId: 'two',
      type: 'CPU',
      duration: 30,
      includeNative: false,
      includeIdle: false
    });
  });

  it('preserves a live selection when the worker list is reordered', () => {
    loadPythonWorkers.mockReturnValue(of([firstWorker, secondWorker]));
    component.ngOnInit();
    component.setTarget('PYTHON');
    component.workerId = secondWorker.id;
    loadPythonWorkers.mockReturnValue(of([secondWorker, firstWorker]));
    refresh.next(true);
    expect(component.workerId).toBe(secondWorker.id);
  });

  it('does not silently switch to another worker after the selected worker exits', () => {
    loadPythonWorkers.mockReturnValue(of([firstWorker, secondWorker]));
    component.ngOnInit();
    component.setTarget('PYTHON');
    component.workerId = secondWorker.id;
    loadPythonWorkers.mockReturnValue(of([firstWorker]));
    refresh.next(true);
    refresh.next(true);
    expect(component.workerId).toBeNull();
    component.createProfilingInstance();
    expect(createProfilingInstance).not.toHaveBeenCalled();
  });

  it('keeps JVM profiling available when worker discovery fails', () => {
    loadPythonWorkers.mockReturnValue(throwError(() => new Error('worker discovery failed')));
    component.ngOnInit();
    component.setTarget('PYTHON');
    expect(component.workerLoadFailed).toBe(true);
    expect(component.isEnabled).toBe(true);
    expect(component.workerId).toBeNull();
    component.setTarget('JVM');
    component.createProfilingInstance();
    expect(createProfilingInstance).toHaveBeenCalledWith('tm-1', { mode: 'ITIMER', duration: 30 });
  });

  it('discards an in-flight worker response after switching back to the JVM', () => {
    const pendingWorkers = new Subject<PythonWorkerInfo[]>();
    loadPythonWorkers.mockReturnValue(pendingWorkers);
    component.ngOnInit();
    component.setTarget('PYTHON');
    component.setTarget('JVM');
    pendingWorkers.next([firstWorker]);
    pendingWorkers.complete();
    expect(component.target).toBe('JVM');
    expect(component.pythonWorkers).toEqual([]);
    expect(component.workerId).toBeNull();
    expect(component.isLoading).toBe(false);
  });
});

describe('TaskManagerProfilerComponent rendering', () => {
  let fixture: ComponentFixture<TaskManagerProfilerComponent>;
  let element: HTMLElement;
  const worker: PythonWorkerInfo = { id: 'registered', pid: 123, memoryAttachAvailable: false, operators: [] };
  const loadPythonWorkers = vi.fn();
  const createProfilingInstance = vi.fn();
  const profile: ProfilingDetail = {
    status: 'FINISHED',
    triggerTime: 1000,
    finishedTime: 2000,
    duration: 1,
    message: 'Profiling Successful',
    outputFile: 'python-test.svg',
    mode: null,
    worker,
    includeNative: true,
    includeIdle: true
  };

  beforeEach(async () => {
    loadPythonWorkers.mockReset().mockReturnValue(of([worker]));
    createProfilingInstance.mockReset().mockReturnValue(of({ ...profile, status: 'RUNNING' }));
    await TestBed.configureTestingModule({
      imports: [TaskManagerProfilerComponent],
      providers: [
        provideNzIcons(APP_ICONS),
        {
          provide: TaskManagerService,
          useValue: {
            loadProfilingList: () => of({ profilingList: [profile] }),
            loadPythonWorkers,
            createProfilingInstance
          }
        },
        { provide: StatusService, useValue: { refresh$: new Subject<boolean>() } },
        { provide: ActivatedRoute, useValue: activatedRoute },
        { provide: NzMessageService, useValue: { warning: vi.fn() } }
      ]
    }).compileComponents();
    fixture = TestBed.createComponent(TaskManagerProfilerComponent);
    element = fixture.nativeElement;
    fixture.detectChanges();
  });

  it('renders Python options and the requested sampling mode in history', async () => {
    fixture.componentInstance.setTarget('PYTHON');
    fixture.detectChanges();
    await fixture.whenStable();
    fixture.detectChanges();
    expect(element.querySelector('nz-select[name="mode"]')).toBeNull();
    expect(element.querySelector('nz-select[name="worker"]')?.textContent).toContain('PID 123');
    expect(element.textContent).toContain('Include native stacks');
    expect(element.textContent).toContain('CPU + native');
    expect(element.textContent).toContain('Python PID 123');
    const nativeToggle = element.querySelector<HTMLInputElement>('label[name="includeNative"] input');
    nativeToggle!.click();
    fixture.detectChanges();
    element.querySelector<HTMLButtonElement>('button[nz-button]')!.click();
    expect(createProfilingInstance).toHaveBeenCalledWith('tm-1', {
      workerId: 'registered',
      type: 'CPU',
      duration: 30,
      includeNative: true,
      includeIdle: false
    });
  });

  it('disables creation and explains when no Python worker is available', () => {
    loadPythonWorkers.mockReturnValue(of([]));
    fixture.componentInstance.setTarget('PYTHON');
    fixture.detectChanges();
    expect(element.querySelector<HTMLButtonElement>('button[nz-button]')?.disabled).toBe(true);
    expect(element.textContent).toContain('No standalone Python workers are available');
  });
});
