
lazy val root = (project in file(".")).
    settings(
        name := "Hall1960",
        libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.2")
    )
