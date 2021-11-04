package com.xiaohu.bean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@NoArgsConstructor
public class Movie{
        private String id;
        private String movie_name;

        public Movie(String id, String movie_name) {
                this.id = id;
                this.movie_name = movie_name;
        }
}
