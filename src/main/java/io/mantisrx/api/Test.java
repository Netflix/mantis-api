/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.api;

import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;


public class Test {

    private static Observable<Long> getObservable() {
        return Observable
                .create(new Observable.OnSubscribe<Long>() {
                    @Override
                    public void call(final Subscriber<? super Long> subscriber) {
                        Observable
                                .interval(1, TimeUnit.SECONDS)
                                .subscribe(subscriber);
                    }
                })
                .share();
    }

    private static Observable<Observable<Long>> getOofO() {
        return Observable.just(getObservable());
    }

    public static void main(String[] args) {
        final Observable<Observable<Long>> oofO = getOofO();
        oofO
                .flatMap(new Func1<Observable<Long>, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(Observable<Long> longObservable) {
                        return longObservable;
                    }
                })
                .take(6)
                .subscribe(new Action1<Long>() {
                    @Override
                    public void call(Long aLong) {
                        System.out.println(aLong);
                    }
                });
        try {Thread.sleep(3000);} catch (InterruptedException ie) {}
        getOofO()
                .flatMap(new Func1<Observable<Long>, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(Observable<Long> longObservable) {
                        return longObservable;
                    }
                })
                .subscribe(new Action1<Long>() {
                    @Override
                    public void call(Long aLong) {
                        System.out.println("        " + aLong);
                    }
                });
        try {Thread.sleep(10000);} catch (InterruptedException ie) {}
    }

    public static void old_main(String[] args) {
        final Observable<Long> o = getObservable();
        o
                .take(6)
                .subscribe(new Action1<Long>() {
                    @Override
                    public void call(Long aLong) {
                        System.out.println(aLong);
                    }
                });
        try {Thread.sleep(3000);} catch (InterruptedException ie) {}
        getObservable()
                .subscribe(new Action1<Long>() {
                    @Override
                    public void call(Long aLong) {
                        System.out.println("        " + aLong);
                    }
                });
        o
                .subscribe(new Action1<Long>() {
                    @Override
                    public void call(Long aLong) {
                        System.out.println("                " + aLong);
                    }
                });
        try {Thread.sleep(10000);} catch (InterruptedException ie) {}
    }
}
