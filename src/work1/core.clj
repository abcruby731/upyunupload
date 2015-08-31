(ns work1.core
  (:require [csv-map.core :refer [parse-csv write-csv]]
            [clojure.java.jdbc :as jdbc]
            [org.httpkit.server :as kit :refer [run-server]]
            [org.httpkit.client :as http]
            [clj-time.core :as time]
            [clj-time.format]
            [clojure.string :as s]
            [clojure.java.io :as io :refer [input-stream file]])
  (:import [java.security.MessageDigest]
           [java.math.BigInteger]))

(let [db-host "localhost"
      db-port 5432
      db-name "workdb"]
  (def db-spec {:subprotocol "postgresql"
                :subname (str "//"  db-host ":" db-port "/" db-name)
                :user "aaa"
                :password "123"}))

(defn md5 [s]
  (let [algorithm (java.security.MessageDigest/getInstance "MD5")
        size (* 2 (.getDigestLength algorithm))
        raw (.digest algorithm (.getBytes s))
        sig (.toString (java.math.BigInteger. 1 raw) 16)
        padding (apply str (repeat (- size (count sig)) "0"))]
    (str padding sig)))

(defn sign [method uri date content-length password]
  (-> (s/join '& [method uri date content-length (md5 password)])
      md5))

(defn to-GMT []
  (clj-time.format/unparse (clj-time.format/formatter "EEE, dd MMM yyyy HH:mm:ss 'GMT'") (time/now)))


(defn insert-db [tablename arg-map]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/insert! conn tablename arg-map)))

(defn query-from-db [psql]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/query conn psql)))

(defn delete-from-db [tablename psql]
  (jdbc/with-db-connection [conn db-spec]
    (jdbc/delete! conn tablename psql)))

(defn app [req]
  {:status 200
   :headers {"Content-Type" "text/plain"}
   :body req})

;; (defn certify [bucket user_id pass]
;;   (http/get "http://v0.api.upyun.com/bucket"
;;             {:headers {"Authorization" ""}}))

;; (defn filename [local-path]
;;   (last (s/split local-path #"/")))

;; (defn get-path [file] (.getPath file))
;; (defn isfile? [file] (not (.isDirectory file)))
(defn all-file-path
  "得到一个文件夹下所有文件的绝对路径（包括子文件夹包含的文件）"
  [absolute-path]
  (->> (io/file absolute-path)
      file-seq
      (filter #(not (.isDirectory %)))
      (map #(.getPath %))))



(defn uploadFile [operator_id password path]
  (let [file (file path)
        uri (str "/test-hoolay/" (System/currentTimeMillis) ".jpg")
        url (str "http://v0.api.upyun.com" uri)
        date (to-GMT)
        filelength (.length file)
        _ (prn uri)
        result @(http/put url
                          {:headers {"Authorization" (str "UpYun " operator_id ":" (sign "PUT" uri date filelength password))
                                     "Date" date
                                     "Content-Length" (str filelength)}
                           :body (input-stream file)})
        _ (prn result)]
    (if (= (:status result) 200)
      (let [{:keys [x-upyun-width x-upyun-height x-upyun-frames x-upyun-file-type]} (:headers result)
            path-vec (s/split path #"/")
            college-id (path-vec 5)
            art-id (path-vec 6)
            filename (last path-vec)
            work-id (first (s/split filename #"-"))
            user-id ((first (query-from-db ["select * from users where 院校序号 = ? and 艺术家序号 = ?" college-id (str (Integer/parseInt art-id))])) :id)]
        (insert-db :testpictures {:user_id user-id, :work_id (Integer/parseInt work-id), :link url, :width (Integer/parseInt x-upyun-width), :height (Integer/parseInt x-upyun-height)})
        ;;insert-db
        {:ok {:width x-upyun-width
              :height x-upyun-height
              :frames x-upyun-frames
              :type x-upyun-file-type
              :url url}})
      {:error (:body result)})
    ))

(defn upload
  [operator_id password path]
  (let [paths (all-file-path path)]
    (doseq [path paths]
      (uploadFile operator_id password path))))

(defn uploadFile+ [operator_id password path]
  (map #(uploadFile operator_id password %) (all-file-path path)))

(defonce server (atom nil))

(defn stop-server []
  (when-not (nil? @server)
    (@server :timeout 100)
    (reset! server nil)))

(defn -main [& args]
  (reset! server (run-server #'app {:port 8080}))
  (prn "Server started!"))
