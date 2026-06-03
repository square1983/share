有，按这个顺序排查最快：

1. 看实际解析到的 Hilt 版本

./gradlew :app:dependencyInsight \
  --configuration kaptDebugKotlinClasspath \
  --dependency hilt-android-compiler

再查 Dagger：

./gradlew :app:dependencyInsight \
  --configuration kaptDebugKotlinClasspath \
  --dependency dagger

如果结果里不是 2.56.2，说明某处还在拉旧版本。

2. 查全部 kapt 依赖

./gradlew :app:dependencies --configuration kaptDebugKotlinClasspath

重点找：

hilt
dagger
kotlin-metadata-jvm
room
realm

3. 全项目搜索旧版本

grep -R "2.5\|2.4\|1.2.0\|hilt\|dagger\|kapt" -n \
  build.gradle app/build.gradle gradle.properties settings.gradle

4. 临时降 Kotlin 验证

把：

kotlin_version = '2.3.0'

临时改成：

kotlin_version = '2.2.0'

如果马上通过，说明就是某个 kapt processor 暂不支持 Kotlin 2.3。

5. 禁用增量 kapt

在 gradle.properties 加：

kapt.incremental.apt=false
kapt.use.worker.api=false

然后：

./gradlew clean :app:kaptDebugKotlin --refresh-dependencies --stacktrace

6. 最直接定位法

运行：

./gradlew :app:kaptDebugKotlin --debug > kapt.log 2>&1
grep -i "processor\|hilt\|dagger\|metadata" kapt.log

能看到到底是哪个 annotation processor 报的。

注意休息。
