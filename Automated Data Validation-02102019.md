
# AMP Analytics Online

#### Input data path for validation :


```scala
val path = "/Users/karanalang/Documents/1.AppleHadoop/AMP-Analytics/parquetFolder/umc_availability/*"
```


    path = /Users/karanalang/Documents/1.AppleHadoop/AMP-Analytics/parquetFolder/umc_availability/*






    /Users/karanalang/Documents/1.AppleHadoop/AMP-Analytics/parquetFolder/umc_availability/*




```scala
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame,SparkSession, Column}
import org.apache.spark.sql.functions.{col}

import scala.collection.mutable.ListBuffer
```


```scala
%AddDeps org.vegas-viz vegas_2.11 0.3.9 --transitive
%AddDeps org.vegas-viz vegas-spark_2.11 0.3.9
```

    Marking org.vegas-viz:vegas_2.11:0.3.9 for download
    Obtained 42 files
    Marking org.vegas-viz:vegas-spark_2.11:0.3.9 for download
    Obtained 2 files



```scala
import vegas._
import vegas.render.WindowRenderer._
import vegas.data.External._
import vegas.sparkExt._

implicit val render = vegas.render.ShowHTML(kernel.display.content("text/html", _))
```


    render = <function1>






    <function1>




```scala
val df = spark.read.parquet(path)
df.printSchema()
```

    root
     |-- content_canonical_id: string (nullable = true)
     |-- reference_id: string (nullable = true)
     |-- service_id: string (nullable = true)
     |-- version_id: string (nullable = true)
     |-- offering_type: string (nullable = true)
     |-- offering_type_value: string (nullable = true)
     |-- offering_type_display_value: string (nullable = true)
     |-- start_time: timestamp (nullable = true)
     |-- end_time: timestamp (nullable = true)
     |-- restrictions: array (nullable = true)
     |    |-- element: struct (containsNull = true)
     |    |    |-- restrictionType: struct (nullable = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- value: string (nullable = true)
     |    |    |    |-- displayValue: string (nullable = true)
     |    |    |-- restrictionRelationship: struct (nullable = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- value: string (nullable = true)
     |    |    |    |-- displayValue: string (nullable = true)
     |    |    |-- value: string (nullable = true)
     |-- is_live: boolean (nullable = true)
     |-- is_broadcast: boolean (nullable = true)
     |-- is_premium: boolean (nullable = true)
     |-- primary_locale: string (nullable = true)
     |-- closed_captioning_locales: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- subtitle_locales: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- dubbed_locales: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- subtitle_deaf_hard_of_hearing_locales: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- audio_track_locales: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- audio_track_formats: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- video_quality: string (nullable = true)
     |-- video_color_range: string (nullable = true)
     |-- created: timestamp (nullable = true)
     |-- etl_batch_sk: long (nullable = true)
     |-- etl_create_ts: long (nullable = true)
     |-- etl_change_ts: long (nullable = true)
     |-- etl_source_batch_sk: string (nullable = true)
    



    df = [content_canonical_id: string, reference_id: string ... 25 more fields]






    [content_canonical_id: string, reference_id: string ... 25 more fields]




```scala
val totalCount = df.count()
println(s" TOTAL NUMBER OF RECORDS IN DATASET : $totalCount")
```

     TOTAL NUMBER OF RECORDS IN DATASET : 513214



    totalCount = 513214






    513214




```scala
val dfColumns = df.columns   
 val lb = ListBuffer[(String, Long, Float)]()
for(colName <- dfColumns){
        val nullCount : Long = df.where(col(colName).isNull).count
        val percentNull = (nullCount.toFloat*100)/totalCount
        val t  = (colName, nullCount, percentNull)
        lb += t
      }

val ll : List[(String, Long, Float)] = lb.toList
val dfCountNull : DataFrame = ll.toDF("colName", "nullCount","percentNull")

dfCountNull.show(30, false)
```

    +-------------------------------------+---------+-----------+
    |colName                              |nullCount|percentNull|
    +-------------------------------------+---------+-----------+
    |content_canonical_id                 |3944     |0.7684903  |
    |reference_id                         |0        |0.0        |
    |service_id                           |0        |0.0        |
    |version_id                           |0        |0.0        |
    |offering_type                        |415614   |80.98259   |
    |offering_type_value                  |415614   |80.98259   |
    |offering_type_display_value          |415614   |80.98259   |
    |start_time                           |415614   |80.98259   |
    |end_time                             |415614   |80.98259   |
    |restrictions                         |496285   |96.70138   |
    |is_live                              |415614   |80.98259   |
    |is_broadcast                         |415614   |80.98259   |
    |is_premium                           |415614   |80.98259   |
    |primary_locale                       |0        |0.0        |
    |closed_captioning_locales            |488294   |95.144325  |
    |subtitle_locales                     |500681   |97.55794   |
    |dubbed_locales                       |512857   |99.930435  |
    |subtitle_deaf_hard_of_hearing_locales|509416   |99.25996   |
    |audio_track_locales                  |504341   |98.271095  |
    |audio_track_formats                  |473159   |92.19527   |
    |video_quality                        |438943   |85.52826   |
    |video_color_range                    |459326   |89.49989   |
    |created                              |0        |0.0        |
    |etl_batch_sk                         |0        |0.0        |
    |etl_create_ts                        |0        |0.0        |
    |etl_change_ts                        |0        |0.0        |
    |etl_source_batch_sk                  |0        |0.0        |
    +-------------------------------------+---------+-----------+
    



    dfColumns = Array(content_canonical_id, reference_id, service_id, version_id, offering_type, offering_type_value, offering_type_display_value, start_time, end_time, restrictions, is_live, is_broadcast, is_premium, primary_locale, closed_captioning_locales, subtitle_locales, dubbed_locales, subtitle_deaf_hard_of_hearing_locales, audio_track_locales, audio_track_formats, video_quality, video_color_range, created, etl_batch_sk, etl_create_ts, etl_change_ts, etl_source_batch_sk)
    lb = ListBuffer((content_canonical_id,3944,0.7684903), (reference_id,0,0.0), (service_id,0,0.0), (version_id,0,0.0), (offering_type,415614,80.98259), (offering_type_value,415614,80.98259), (offering_type_display_value,415614,80.98259), (start...






    ListBuffer((content_canonical_id,3944,0.7684903), (reference_id,0,0.0), (service_id,0,0.0), (version_id,0,0.0), (offering_type,415614,80.98259), (offering_type_value,415614,80.98259), (offering_type_display_value,415614,80.98259), (start...



#### Input values for colX, colY (X and Y axis columns for Null %age visualization) 


```scala
val colX = "colName"
val colY = "percentNull"
```


    colX = colName
    colY = percentNull






    percentNull




```scala
val usingSparkdf = Vegas("UsingSpark")
  .withDataFrame(dfCountNull)
  .encodeX(colX, Nom, scale=Scale(bandSize = 20.0), hideAxis=false)
  .encodeY(colY, Quant)
.encodeColor(colX, Nom)
.filter("datum.percentNull > 0")
  .mark(Bar)

usingSparkdf.show
```



  <iframe id="frame-vegas-9c5ac175-b76a-49c1-8061-a8ee7e374980" sandbox="allow-scripts allow-same-origin" style="border: none; width: 100%" srcdoc="&lt;html&gt;
  &lt;head&gt;
    &lt;script src=&quot;https://cdn.jsdelivr.net/webjars/org.webjars.bower/d3/3.5.17/d3.min.js&quot; charset=&quot;utf-8&quot;&gt;&lt;/script&gt;
&lt;script src=&quot;https://cdn.jsdelivr.net/webjars/org.webjars.bower/vega/2.6.3/vega.min.js&quot; charset=&quot;utf-8&quot;&gt;&lt;/script&gt;
&lt;script src=&quot;https://cdn.jsdelivr.net/webjars/org.webjars.bower/vega-lite/1.2.0/vega-lite.min.js&quot; charset=&quot;utf-8&quot;&gt;&lt;/script&gt;
&lt;script src=&quot;https://vega.github.io/vega-editor/vendor/vega-embed.js&quot; charset=&quot;utf-8&quot;&gt;&lt;/script&gt;
  &lt;/head&gt;
  &lt;body&gt;
 &lt;script&gt;
   var embedSpec = {
     mode: &quot;vega-lite&quot;,
     spec: {
  &quot;mark&quot; : &quot;bar&quot;,
  &quot;encoding&quot; : {
    &quot;x&quot; : {
      &quot;axis&quot; : true,
      &quot;scale&quot; : {
        &quot;bandSize&quot; : 20.0
      },
      &quot;field&quot; : &quot;colName&quot;,
      &quot;type&quot; : &quot;nominal&quot;
    },
    &quot;y&quot; : {
      &quot;field&quot; : &quot;percentNull&quot;,
      &quot;type&quot; : &quot;quantitative&quot;
    },
    &quot;color&quot; : {
      &quot;field&quot; : &quot;colName&quot;,
      &quot;type&quot; : &quot;nominal&quot;
    }
  },
  &quot;description&quot; : &quot;UsingSpark&quot;,
  &quot;data&quot; : {
    &quot;values&quot; : [
      {
        &quot;colName&quot; : &quot;content_canonical_id&quot;,
        &quot;nullCount&quot; : 3944,
        &quot;percentNull&quot; : 0.7684903144836426
      },
      {
        &quot;colName&quot; : &quot;reference_id&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;service_id&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;version_id&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;offering_type&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;offering_type_value&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;offering_type_display_value&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;start_time&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;end_time&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;restrictions&quot;,
        &quot;nullCount&quot; : 496285,
        &quot;percentNull&quot; : 96.70137786865234
      },
      {
        &quot;colName&quot; : &quot;is_live&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;is_broadcast&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;is_premium&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;primary_locale&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;closed_captioning_locales&quot;,
        &quot;nullCount&quot; : 488294,
        &quot;percentNull&quot; : 95.14432525634766
      },
      {
        &quot;colName&quot; : &quot;subtitle_locales&quot;,
        &quot;nullCount&quot; : 500681,
        &quot;percentNull&quot; : 97.55793762207031
      },
      {
        &quot;colName&quot; : &quot;dubbed_locales&quot;,
        &quot;nullCount&quot; : 512857,
        &quot;percentNull&quot; : 99.93043518066406
      },
      {
        &quot;colName&quot; : &quot;subtitle_deaf_hard_of_hearing_locales&quot;,
        &quot;nullCount&quot; : 509416,
        &quot;percentNull&quot; : 99.25995635986328
      },
      {
        &quot;colName&quot; : &quot;audio_track_locales&quot;,
        &quot;nullCount&quot; : 504341,
        &quot;percentNull&quot; : 98.2710952758789
      },
      {
        &quot;colName&quot; : &quot;audio_track_formats&quot;,
        &quot;nullCount&quot; : 473159,
        &quot;percentNull&quot; : 92.19526672363281
      },
      {
        &quot;colName&quot; : &quot;video_quality&quot;,
        &quot;nullCount&quot; : 438943,
        &quot;percentNull&quot; : 85.52825927734375
      },
      {
        &quot;colName&quot; : &quot;video_color_range&quot;,
        &quot;nullCount&quot; : 459326,
        &quot;percentNull&quot; : 89.49989318847656
      },
      {
        &quot;colName&quot; : &quot;created&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;etl_batch_sk&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;etl_create_ts&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;etl_change_ts&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;etl_source_batch_sk&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      }
    ]
  },
  &quot;transform&quot; : {
    &quot;filter&quot; : &quot;datum.percentNull &gt; 0&quot;
  }
}
   }
   vg.embed(&quot;#vegas-9c5ac175-b76a-49c1-8061-a8ee7e374980&quot;, embedSpec, function(error, result) {});
 &lt;/script&gt;
 &lt;div id='vegas-9c5ac175-b76a-49c1-8061-a8ee7e374980'&gt;&lt;/div&gt;
    &lt;/body&gt;
&lt;/html&gt;"></iframe>
  <script>
    if (typeof resizeIFrame != 'function') {
      function resizeIFrame(el, k) {
        $(el.contentWindow.document).ready(function() {
          el.style.height = el.contentWindow.document.body.scrollHeight + 'px';
        });
        if (k <= 10) { setTimeout(function() { resizeIFrame(el, k+1) }, 1000 + (k * 250)) };
      }
    }
    $().ready( function() { resizeIFrame($('#frame-vegas-9c5ac175-b76a-49c1-8061-a8ee7e374980').get(0), 1); });
  </script>
    



    usingSparkdf = ExtendedUnitSpecBuilder(ExtendedUnitSpec(None,None,Bar,Some(Encoding(None,None,Some(PositionChannelDef(Some(AxisBoolean(true)),Some(Scale(None,None,None,None,Some(BandSizeDouble(20.0)),None,None,None,None,None,None)),None,Some(colName),Some(Nominal),None,None,None,None,None)),Some(PositionChannelDef(None,None,None,Some(percentNull),Some(Quantitative),None,None,None,None,None)),None,None,Some(ChannelDefWithLegend(None,None,None,Some(colName),Some(Nominal),None,None,None,None,None)),None,None,None,None,None,None,None,None)),None,Some(UsingSpark),Some(Data(None,None,Some(List(Values(Map(colName -> content_canonical_id, nullCount -> 3944, percentNull -> 0.7684903)), Values(Map(colName -> reference_id, nullCount -> 0, percentNull -> 0.0)), Va...






    ExtendedUnitSpecBuilder(ExtendedUnitSpec(None,None,Bar,Some(Encoding(None,None,Some(PositionChannelDef(Some(AxisBoolean(true)),Some(Scale(None,None,None,None,Some(BandSizeDouble(20.0)),None,None,None,None,None,None)),None,Some(colName),Some(Nominal),None,None,None,None,None)),Some(PositionChannelDef(None,None,None,Some(percentNull),Some(Quantitative),None,None,None,None,None)),None,None,Some(ChannelDefWithLegend(None,None,None,Some(colName),Some(Nominal),None,None,None,None,None)),None,None,None,None,None,None,None,None)),None,Some(UsingSpark),Some(Data(None,None,Some(List(Values(Map(colName -> content_canonical_id, nullCount -> 3944, percentNull -> 0.7684903)), Values(Map(colName -> reference_id, nullCount -> 0, percentNull -> 0.0)), Va...




```scala
val usingSparkdf_circle = Vegas("UsingSpark")
  .withDataFrame(dfCountNull)
  .encodeX(colX, Nom, scale=Scale(bandSize = 20.0), hideAxis=false)
  .encodeY(colY, Quant)
//.encodeColor(colX, Nom)
.filter("datum.percentNull > 0")
  .mark(Circle)

usingSparkdf_circle.show
```



  <iframe id="frame-vegas-2d39158c-f3a8-4be0-a822-8b26e3ca21c9" sandbox="allow-scripts allow-same-origin" style="border: none; width: 100%" srcdoc="&lt;html&gt;
  &lt;head&gt;
    &lt;script src=&quot;https://cdn.jsdelivr.net/webjars/org.webjars.bower/d3/3.5.17/d3.min.js&quot; charset=&quot;utf-8&quot;&gt;&lt;/script&gt;
&lt;script src=&quot;https://cdn.jsdelivr.net/webjars/org.webjars.bower/vega/2.6.3/vega.min.js&quot; charset=&quot;utf-8&quot;&gt;&lt;/script&gt;
&lt;script src=&quot;https://cdn.jsdelivr.net/webjars/org.webjars.bower/vega-lite/1.2.0/vega-lite.min.js&quot; charset=&quot;utf-8&quot;&gt;&lt;/script&gt;
&lt;script src=&quot;https://vega.github.io/vega-editor/vendor/vega-embed.js&quot; charset=&quot;utf-8&quot;&gt;&lt;/script&gt;
  &lt;/head&gt;
  &lt;body&gt;
 &lt;script&gt;
   var embedSpec = {
     mode: &quot;vega-lite&quot;,
     spec: {
  &quot;mark&quot; : &quot;circle&quot;,
  &quot;encoding&quot; : {
    &quot;x&quot; : {
      &quot;axis&quot; : true,
      &quot;scale&quot; : {
        &quot;bandSize&quot; : 20.0
      },
      &quot;field&quot; : &quot;colName&quot;,
      &quot;type&quot; : &quot;nominal&quot;
    },
    &quot;y&quot; : {
      &quot;field&quot; : &quot;percentNull&quot;,
      &quot;type&quot; : &quot;quantitative&quot;
    }
  },
  &quot;description&quot; : &quot;UsingSpark&quot;,
  &quot;data&quot; : {
    &quot;values&quot; : [
      {
        &quot;colName&quot; : &quot;content_canonical_id&quot;,
        &quot;nullCount&quot; : 3944,
        &quot;percentNull&quot; : 0.7684903144836426
      },
      {
        &quot;colName&quot; : &quot;reference_id&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;service_id&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;version_id&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;offering_type&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;offering_type_value&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;offering_type_display_value&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;start_time&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;end_time&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;restrictions&quot;,
        &quot;nullCount&quot; : 496285,
        &quot;percentNull&quot; : 96.70137786865234
      },
      {
        &quot;colName&quot; : &quot;is_live&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;is_broadcast&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;is_premium&quot;,
        &quot;nullCount&quot; : 415614,
        &quot;percentNull&quot; : 80.98258972167969
      },
      {
        &quot;colName&quot; : &quot;primary_locale&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;closed_captioning_locales&quot;,
        &quot;nullCount&quot; : 488294,
        &quot;percentNull&quot; : 95.14432525634766
      },
      {
        &quot;colName&quot; : &quot;subtitle_locales&quot;,
        &quot;nullCount&quot; : 500681,
        &quot;percentNull&quot; : 97.55793762207031
      },
      {
        &quot;colName&quot; : &quot;dubbed_locales&quot;,
        &quot;nullCount&quot; : 512857,
        &quot;percentNull&quot; : 99.93043518066406
      },
      {
        &quot;colName&quot; : &quot;subtitle_deaf_hard_of_hearing_locales&quot;,
        &quot;nullCount&quot; : 509416,
        &quot;percentNull&quot; : 99.25995635986328
      },
      {
        &quot;colName&quot; : &quot;audio_track_locales&quot;,
        &quot;nullCount&quot; : 504341,
        &quot;percentNull&quot; : 98.2710952758789
      },
      {
        &quot;colName&quot; : &quot;audio_track_formats&quot;,
        &quot;nullCount&quot; : 473159,
        &quot;percentNull&quot; : 92.19526672363281
      },
      {
        &quot;colName&quot; : &quot;video_quality&quot;,
        &quot;nullCount&quot; : 438943,
        &quot;percentNull&quot; : 85.52825927734375
      },
      {
        &quot;colName&quot; : &quot;video_color_range&quot;,
        &quot;nullCount&quot; : 459326,
        &quot;percentNull&quot; : 89.49989318847656
      },
      {
        &quot;colName&quot; : &quot;created&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;etl_batch_sk&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;etl_create_ts&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;etl_change_ts&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      },
      {
        &quot;colName&quot; : &quot;etl_source_batch_sk&quot;,
        &quot;nullCount&quot; : 0,
        &quot;percentNull&quot; : 0.0
      }
    ]
  },
  &quot;transform&quot; : {
    &quot;filter&quot; : &quot;datum.percentNull &gt; 0&quot;
  }
}
   }
   vg.embed(&quot;#vegas-2d39158c-f3a8-4be0-a822-8b26e3ca21c9&quot;, embedSpec, function(error, result) {});
 &lt;/script&gt;
 &lt;div id='vegas-2d39158c-f3a8-4be0-a822-8b26e3ca21c9'&gt;&lt;/div&gt;
    &lt;/body&gt;
&lt;/html&gt;"></iframe>
  <script>
    if (typeof resizeIFrame != 'function') {
      function resizeIFrame(el, k) {
        $(el.contentWindow.document).ready(function() {
          el.style.height = el.contentWindow.document.body.scrollHeight + 'px';
        });
        if (k <= 10) { setTimeout(function() { resizeIFrame(el, k+1) }, 1000 + (k * 250)) };
      }
    }
    $().ready( function() { resizeIFrame($('#frame-vegas-2d39158c-f3a8-4be0-a822-8b26e3ca21c9').get(0), 1); });
  </script>
    



    usingSparkdf_circle = ExtendedUnitSpecBuilder(ExtendedUnitSpec(None,None,Circle,Some(Encoding(None,None,Some(PositionChannelDef(Some(AxisBoolean(true)),Some(Scale(None,None,None,None,Some(BandSizeDouble(20.0)),None,None,None,None,None,None)),None,Some(colName),Some(Nominal),None,None,None,None,None)),Some(PositionChannelDef(None,None,None,Some(percentNull),Some(Quantitative),None,None,None,None,None)),None,None,None,None,None,None,None,None,None,None,None)),None,Some(UsingSpark),Some(Data(None,None,Some(List(Values(Map(colName -> content_canonical_id, nullCount -> 3944, percentNull -> 0.7684903)), Values(Map(colName -> reference_id, nullCount -> 0, percentNull -> 0.0)), Values(Map(colName -> service_id, nullCount -> 0, percentNull -> 0.0)), Values(Map(...






    ExtendedUnitSpecBuilder(ExtendedUnitSpec(None,None,Circle,Some(Encoding(None,None,Some(PositionChannelDef(Some(AxisBoolean(true)),Some(Scale(None,None,None,None,Some(BandSizeDouble(20.0)),None,None,None,None,None,None)),None,Some(colName),Some(Nominal),None,None,None,None,None)),Some(PositionChannelDef(None,None,None,Some(percentNull),Some(Quantitative),None,None,None,None,None)),None,None,None,None,None,None,None,None,None,None,None)),None,Some(UsingSpark),Some(Data(None,None,Some(List(Values(Map(colName -> content_canonical_id, nullCount -> 3944, percentNull -> 0.7684903)), Values(Map(colName -> reference_id, nullCount -> 0, percentNull -> 0.0)), Values(Map(colName -> service_id, nullCount -> 0, percentNull -> 0.0)), Values(Map(...


